import {
  bucketTtlFn,
  finiteNumberFn,
  lazyRefill,
  redisNow,
  slotAccounting,
  usableConcurrencyFn,
  usableCostFn,
  usableRequestTtlFn,
} from "./fragments.js";

/**
 * Several budgets claimed as one operation. Twin of the memory backend's
 * `acquireMultiLimit` / `planMultiLimit` pair.
 *
 * These three scripts are the only ones registered without a fixed key count,
 * because the number of budgets is a property of the client's configuration
 * rather than of the script. Every budget key is declared in `KEYS`; its
 * `:costs` companion is derived inside the script, as every slot script derives
 * it — see docs/design-notes.md#the-redis-backend-requires-one-logical-keyspace.
 *
 * `ARGV[1]` is the cost, `ARGV[2]` the spec list as JSON and, in the two
 * acquiring scripts, `ARGV[3]` the key TTL in seconds (a bucket's is raised to
 * outlive its refill by `bucketTtl`). A spec is one object per budget, holding
 * the kind, the 1-based `KEYS` index it lives at, and the configuration to
 * meter it against; `tryAdmitMultiLimit` wraps the list as
 * `{ specs, freezeKeys }`. A positional ARGV per field would renumber every
 * slot whenever a budget was added — see `ttlArgvWarning` in `fragments.ts`.
 */

/**
 * The check phase both acquiring scripts share, declared as a function so the
 * two agree on the arithmetic character for character.
 *
 * Refill credit is written whether or not the request gets to spend it: the
 * intervals really did elapse. Slot reaping is likewise unconditional — an
 * expired slot is expired whoever asked.
 *
 * Requires: `KEYS`, `finiteNumber`, `bucketTtl`, `usableConcurrency`,
 * `usableCost` and `usableRequestTtl`.
 * Declares `planMultiLimit`, `commitMultiLimit` and `persistRefills`.
 */
const planFn = `
local function planMultiLimit(specs, cost, now, ttl)
  local plan = { acquired = true, buckets = {}, slots = {} }
  for _, spec in ipairs(specs) do
    local key = KEYS[spec.k]
    if spec.kind == 'tokenBucket' then
      -- tonumber: a non-finite value arrives as a string; see wire() in
      -- ops/multiLimit.ts.
      local maxTokens = tonumber(spec.maxTokens)
      local tokensToAdd = tonumber(spec.tokensToAdd)
      local interval = tonumber(spec.interval)
      -- The same four refusals the single-budget token bucket makes, in the same
      -- order, so one unusable entry is reported rather than metered.
      if tokensToAdd == nil or not (tokensToAdd > 0) then
        return { acquired = false, error = 'tokensToAdd must be greater than 0' }
      end
      if maxTokens == nil or not (maxTokens > 0) or maxTokens == math.huge then
        return { acquired = false, error = 'maxTokens must be a finite number greater than 0' }
      end
      if cost == nil or cost ~= cost or cost == math.huge or cost == -math.huge then
        return { acquired = false, error = 'cost must be a finite number' }
      end
      if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then
        return { acquired = false, error = 'interval must be a finite number' }
      end

      local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), maxTokens)
      local lastUpdate = finiteNumber(redis.call('HGET', key, 'lastUpdate'), now)

${lazyRefill("      ")}

      table.insert(plan.buckets, {
        key = key,
        tokens = tokens,
        lastUpdate = lastUpdate,
        ttl = bucketTtl(ttl, maxTokens, tokensToAdd, interval),
      })
      if tokens < cost then
        local intervalsNeeded = math.ceil((cost - tokens) / tokensToAdd)
        local wait = intervalsNeeded * interval - (now - lastUpdate)
        if wait < 0 then wait = 0 end
        -- The longest wait any budget asks for: waking sooner only to be refused
        -- by this same budget is a wake-up that cannot succeed.
        if plan.waitTime == nil or wait > plan.waitTime then
          plan.waitTime = wait
          plan.blockedBy = key
        end
        plan.acquired = false
      end
    else
      local maxConcurrency = tonumber(spec.maxConcurrency)
      local requestTtl = tonumber(spec.requestTtl)
      local requestId = spec.slotId
      if not usableConcurrency(maxConcurrency) then
        return { acquired = false, error = 'maxConcurrency must be a finite number greater than 0' }
      end
      if not usableCost(cost) then
        return { acquired = false, error = 'cost must be a finite number that is not negative' }
      end
      if not usableRequestTtl(requestTtl) then
        return { acquired = false, error = 'requestTtl must be a finite number greater than 0' }
      end

${slotAccounting("      ")}

      if currentCost + cost > maxConcurrency then
        plan.acquired = false
        -- No deadline of its own: a slot is freed by a completion, and the caller
        -- decides what to fall back on. Recorded only if nothing with a real wait
        -- already claimed the report.
        if plan.blockedBy == nil then plan.blockedBy = key end
      else
        table.insert(plan.slots, { key = key, slotId = requestId })
      end
    end
  end
  return plan
end

local function commitMultiLimit(plan, cost, now, ttl)
  for _, bucket in ipairs(plan.buckets) do
    redis.call('HSET', bucket.key, 'tokens', bucket.tokens - cost, 'lastUpdate', bucket.lastUpdate)
    redis.call('EXPIRE', bucket.key, bucket.ttl)
  end
  for _, slot in ipairs(plan.slots) do
    redis.call('ZADD', slot.key, now, slot.slotId)
    redis.call('HSET', slot.key .. ':costs', slot.slotId, cost)
    redis.call('EXPIRE', slot.key, ttl)
    redis.call('EXPIRE', slot.key .. ':costs', ttl)
  end
end

local function persistRefills(plan)
  for _, bucket in ipairs(plan.buckets) do
    redis.call('HSET', bucket.key, 'tokens', bucket.tokens, 'lastUpdate', bucket.lastUpdate)
    redis.call('EXPIRE', bucket.key, bucket.ttl)
  end
end
`;

export const acquireMultiLimit = `
  local cost = tonumber(ARGV[1])
  local specs = cjson.decode(ARGV[2])
  local ttl = tonumber(ARGV[3]) or 86400

${redisNow()}

${finiteNumberFn()}

${bucketTtlFn()}

${usableConcurrencyFn()}

${usableCostFn()}

${usableRequestTtlFn()}

${planFn}

  local plan = planMultiLimit(specs, cost, now, ttl)
  if plan.error then
    return cjson.encode({ acquired = false, error = plan.error })
  end
  if not plan.acquired then
    persistRefills(plan)
    return cjson.encode({ acquired = false, waitTime = plan.waitTime or 0, blockedBy = plan.blockedBy })
  end
  commitMultiLimit(plan, cost, now, ttl)
  return cjson.encode({ acquired = true })
`;

/**
 * The no-queue fast path across several budgets.
 *
 * `KEYS[1]` is the queue; the freeze keys and the budgets follow, addressed by
 * the indices in `ARGV[2]`. Refusing while anything is queued and while any
 * governing freeze stands are both part of the same atomic decision, for the
 * reason `tryAdmitImmediately` gives: answering them separately lets a peer
 * enqueue in between and be overtaken.
 */
export const tryAdmitMultiLimit = `
  local queueKey = KEYS[1]
  local cost = tonumber(ARGV[1])
  local payload = cjson.decode(ARGV[2])
  local ttl = tonumber(ARGV[3]) or 86400
  local specs = payload.specs

${redisNow()}

${finiteNumberFn()}

${bucketTtlFn()}

${usableConcurrencyFn()}

${usableCostFn()}

${usableRequestTtlFn()}

${planFn}

  if redis.call('ZCARD', queueKey) > 0 then return 0 end

  for _, index in ipairs(payload.freezeKeys) do
    local freeze = redis.call('HMGET', KEYS[index], 'frozenUntil', 'thawRequestCount')
    local frozenUntil = finiteNumber(freeze[1], nil)
    if frozenUntil ~= nil then
      if now < frozenUntil then return 0 end
      -- Once the freeze lapses with probes outstanding, recovery is one request
      -- at a time and only the queue path can arrange that.
      if finiteNumber(freeze[2], 0) > 0 then return 0 end
    end
  end

  local plan = planMultiLimit(specs, cost, now, ttl)
  if plan.error then return 0 end
  if not plan.acquired then
    persistRefills(plan)
    return 0
  end
  commitMultiLimit(plan, cost, now, ttl)
  return 1
`;

/**
 * Hands back what `acquireMultiLimit` claimed.
 *
 * A token refund is refused while the freeze governing that bucket stands, for
 * the reason `refundTokens` gives: the freeze empties buckets on purpose, so
 * crediting one hands back budget the fleet has just been told not to spend. A
 * slot is released either way — nothing else will, and a held slot is not
 * budget the freeze meant to withhold.
 */
export const releaseMultiLimit = `
  local cost = tonumber(ARGV[1])
  local specs = cjson.decode(ARGV[2])

${redisNow()}

${finiteNumberFn()}

  for _, spec in ipairs(specs) do
    local key = KEYS[spec.k]
    if spec.kind == 'concurrency' then
      redis.call('ZREM', key, spec.slotId)
      redis.call('HDEL', key .. ':costs', spec.slotId)
    else
      local frozen = false
      if spec.fk ~= nil then
        local frozenUntil = finiteNumber(redis.call('HGET', KEYS[spec.fk], 'frozenUntil'), 0)
        if frozenUntil > now then frozen = true end
      end
      if not frozen then
        -- An absent bucket stays absent: an absent bucket already reads as full,
        -- so creating one here would invent a balance rather than restore it.
        local raw = redis.call('HGET', key, 'tokens')
        if raw ~= false then
          local tokens = finiteNumber(raw, 0)
          redis.call('HSET', key, 'tokens', math.min(tonumber(spec.maxTokens), tokens + cost))
        end
      end
    end
  end
  return 1
`;
