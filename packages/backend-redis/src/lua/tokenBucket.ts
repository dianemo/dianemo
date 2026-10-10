import {
  bucketTtlFn,
  exactFn,
  finiteNumberFn,
  freezeGate,
  lazyRefill,
  queueEmptyGate,
  redisNow,
  usableCostFn,
  writeMarkFn,
} from "./fragments.js";

/**
 * Token-bucket Lua. Refill is lazy — a request credits the whole intervals
 * elapsed since `lastUpdate` rather than a timer adding tokens — which is why
 * `lastUpdate` advances in interval-sized steps and not to `now`.
 *
 * Twin of the memory backend's token-bucket section.
 */

export const tokenBucket = `
  local key = KEYS[1]
  -- '' when the claim asks for no mark.
  local markKey = KEYS[2]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])
  local tokensToAdd = tonumber(ARGV[3])
  local interval = tonumber(ARGV[4])
  -- ARGV is positional; see ttlArgvWarning in fragments.ts for the trap.
  local ttl = tonumber(ARGV[5]) or 86400
  local markTtl = tonumber(ARGV[6]) or 0

${redisNow()}

${finiteNumberFn()}
${usableCostFn()}
${exactFn()}

${writeMarkFn()}
  -- 'not (x > 0)', not 'x <= 0': a nan compares false either way round, so the
  -- second form admits it and the division below yields a nan balance. The nil
  -- test first, because comparing nil raises.
  if tokensToAdd == nil or not (tokensToAdd > 0) then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'tokensToAdd must be greater than 0' })
  end
  -- Reported through 'error' rather than raised: the contract says an unusable
  -- configuration is distinguishable from a merely empty bucket, and a non-finite
  -- maxTokens or cost would otherwise reach cjson.encode and throw.
  if maxTokens == nil or not (maxTokens > 0) or maxTokens == math.huge then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'maxTokens must be a finite number greater than 0' })
  end
  if not usableCost(cost) then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'cost must be a finite number that is not negative' })
  end
  -- Guarded even though the refill below tolerates it: waitTime is derived from
  -- interval, and a non-finite one clears the '< 0' clamp on the way to
  -- cjson.encode.
  if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then
    return cjson.encode({ acquired = false, waitTime = 0, remainingTokens = 0, error = 'interval must be a finite number' })
  end

${bucketTtlFn()}
  ttl = bucketTtl(ttl, maxTokens, tokensToAdd, interval)

  -- finiteNumber, not tonumber: a stored 'nan' passes a nil test, survives every
  -- comparison and math.min, and then fails in cjson.encode below — so one corrupt
  -- byte would make every acquire on this bucket throw, with nothing rewriting
  -- the field.
  local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), maxTokens)
  local lastUpdate = finiteNumber(redis.call('HGET', key, 'lastUpdate'), now)
  local overflow = finiteNumber(redis.call('HGET', key, 'overflow'), 0)
  local fills = finiteNumber(redis.call('HGET', key, 'fills'), 0)

${lazyRefill()}

  if tokens >= cost then
    tokens = tokens - cost
    redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', lastUpdate, 'overflow', overflow, 'fills', fills)
    redis.call('EXPIRE', key, ttl)
    if markKey ~= '' then writeMark(markKey, markTtl, cost, overflow, fills, key) end
    return cjson.encode({ acquired = true, remainingTokens = exact(tokens) })
  else
    -- lastUpdate only advances in whole intervals, so (now - lastUpdate) is
    -- progress already made toward the next refill and comes off the wait. The
    -- caller sleeps on this value directly.
    local needed = cost - tokens
    local intervalsNeeded = math.ceil(needed / tokensToAdd)
    local waitTime = intervalsNeeded * interval - (now - lastUpdate)
    if waitTime < 0 then waitTime = 0 end

    -- Refill progress is persisted even on refusal.
    redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', lastUpdate, 'overflow', overflow, 'fills', fills)
    redis.call('EXPIRE', key, ttl)

    return cjson.encode({ acquired = false, waitTime = exact(waitTime), remainingTokens = exact(tokens) })
  end
`;

/**
 * Refuses while a freeze that emptied the bucket is in force: crediting a
 * refund into it would hand back budget the fleet has just been told not to
 * spend. A freeze armed with `emptiesBucket: false` left the bucket alone, so a
 * refund under it is credited as usual. A missing bucket is left missing rather than
 * created — an absent bucket already reads as full.
 *
 * `lastUpdate` is deliberately untouched. It marks where refill accounting has
 * reached, and moving it would either forfeit or duplicate accrued intervals.
 */
export const refundTokens = `
  local key = KEYS[1]
  local freezeKey = KEYS[2]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])
${finiteNumberFn()}
${usableCostFn()}

  if not usableCost(cost) then return 0 end

  -- Refused only under a standing freeze that emptied the bucket; an absent
  -- emptiesBucket is a freeze from before the field existed, and those all did.
  -- finiteNumber, as the memory twin's parseFiniteStored: an unusable deadline
  -- is no freeze, as every freeze gate reads it.
  if freezeKey ~= '' then
    local freeze = redis.call('HMGET', freezeKey, 'frozenUntil', 'emptiesBucket')
${redisNow("    ")}
    if finiteNumber(freeze[1], 0) > now and freeze[2] ~= '0' then return 0 end
  end

  -- The raw reply distinguishes an absent bucket, which must stay absent because an
  -- absent bucket already reads as full. Parseability is then a separate question:
  -- 'abc' is truthy in Lua, so testing the string alone let a nil reach the addition.
  local raw = redis.call('HGET', key, 'tokens')
  if raw == false then return 0 end
  local tokens = finiteNumber(raw, 0)
  redis.call('HSET', key, 'tokens', math.min(maxTokens, tokens + cost))
  return 1
`;

/**
 * Writes a full bucket stamped with Redis's clock, in one step: reading `TIME`
 * and then writing is two commands an acquire can land between, and its spend
 * would be overwritten.
 */
export const resetTokenBucket = `
  local key = KEYS[1]
  local maxTokens = ARGV[1]
${redisNow()}
  redis.call('HSET', key, 'tokens', maxTokens, 'lastUpdate', now)
  -- Ends every outstanding settlement: the balance it was owed against is gone.
  redis.call('HINCRBY', key, 'generation', 1)
  redis.call('EXPIRE', key, 86400)
  return 1
`;

/**
 * Admits without queueing when nothing is contending, since an empty queue has no
 * ordering to arrange. One atomic script — refuse if anything is queued, refuse if
 * frozen, else spend — so the fast path can never overtake queued work or
 * double-spend. Uses the refill arithmetic of `tokenBucket` above verbatim, so
 * both paths agree on the bucket.
 */
export const tryAdmitImmediately = `
  local queueKey = KEYS[1]
  local bucketKey = KEYS[2]
  local freezeKey = KEYS[3]
  -- '' when the claim asks for no mark.
  local markKey = KEYS[4]
  local cost = tonumber(ARGV[1])
  local maxTokens = tonumber(ARGV[2])
  local tokensToAdd = tonumber(ARGV[3])
  local interval = tonumber(ARGV[4])
  -- ARGV is positional; see ttlArgvWarning in fragments.ts for the trap.
  local ttl = tonumber(ARGV[5]) or 86400
  local markTtl = tonumber(ARGV[6]) or 0

${redisNow()}

${finiteNumberFn()}
${usableCostFn()}

${writeMarkFn()}
  if tokensToAdd == nil or not (tokensToAdd > 0) then return 0 end
  if maxTokens == nil or not (maxTokens > 0) or maxTokens == math.huge then return 0 end
  if not usableCost(cost) then return 0 end
  if interval == nil or interval ~= interval or interval == math.huge or interval == -math.huge then return 0 end

${bucketTtlFn()}
  ttl = bucketTtl(ttl, maxTokens, tokensToAdd, interval)

${queueEmptyGate()}

${freezeGate()}

  -- Same reading as the queue path's, so both agree on one bucket: a stored 'nan'
  -- otherwise left this script admitting every request unmetered.
  local tokens = finiteNumber(redis.call('HGET', bucketKey, 'tokens'), maxTokens)
  local lastUpdate = finiteNumber(redis.call('HGET', bucketKey, 'lastUpdate'), now)
  local overflow = finiteNumber(redis.call('HGET', bucketKey, 'overflow'), 0)
  local fills = finiteNumber(redis.call('HGET', bucketKey, 'fills'), 0)

${lazyRefill()}

  if tokens < cost then
    redis.call('HSET', bucketKey, 'tokens', tokens, 'lastUpdate', lastUpdate, 'overflow', overflow, 'fills', fills)
    redis.call('EXPIRE', bucketKey, ttl)
    return 0
  end

  tokens = tokens - cost
  redis.call('HSET', bucketKey, 'tokens', tokens, 'lastUpdate', lastUpdate, 'overflow', overflow, 'fills', fills)
  redis.call('EXPIRE', bucketKey, ttl)
  if markKey ~= '' then writeMark(markKey, markTtl, cost, overflow, fills, bucketKey) end
  return 1
`;

/**
 * Twin of the memory backend's `emptyTokenBucket`; the contract is on
 * `DianemoBackend.emptyTokenBucket`.
 */
export const emptyTokenBucket = `
  local key = KEYS[1]
  local maxTokens = tonumber(ARGV[1])
  local tokensToAdd = tonumber(ARGV[2])
  local interval = tonumber(ARGV[3])
  local ttl = tonumber(ARGV[4]) or 86400

${redisNow()}

${finiteNumberFn()}

${bucketTtlFn()}

  local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), 0)
  if tokens > 0 then tokens = 0 end
  local generation = finiteNumber(redis.call('HGET', key, 'generation'), 0) + 1
  redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', now, 'generation', generation)
  if tokensToAdd ~= nil and tokensToAdd > 0 and maxTokens ~= nil and maxTokens > 0
    and maxTokens ~= math.huge and interval ~= nil and interval == interval
    and interval ~= math.huge then
    ttl = bucketTtl(ttl, maxTokens, tokensToAdd, interval)
  end
  redis.call('EXPIRE', key, ttl)
  return 1
`;

/**
 * Corrects each claim to what its response reported, in one script. Twin of
 * `settleAmount` in the core package's `backend/settlement.ts`, which the
 * memory backend calls: the two must agree line for line.
 *
 * Variadic, like the multi-limit scripts: `ARGV[1]` is one JSON object per
 * bucket, addressing the bucket (`k`) and its mark (`mk`) by `KEYS`
 * position. Numbers that may be non-finite arrive as strings, as there.
 */
export const settleTokens = `
  local entries = cjson.decode(ARGV[1])
  local baseTtl = tonumber(ARGV[2]) or 86400

${redisNow()}

${finiteNumberFn()}

${bucketTtlFn()}

  local results = {}
  for i, entry in ipairs(entries) do
    local key = KEYS[entry.k]
    local markKey = KEYS[entry.mk]
    local actual = tonumber(entry.actual)
    local maxTokens = tonumber(entry.maxTokens)
    local tokensToAdd = tonumber(entry.tokensToAdd)
    local interval = tonumber(entry.interval)
    local maxDebt = tonumber(entry.maxDebt)
    local result = { settled = false, applied = 0, unapplied = 0 }

    local mark = redis.call('HMGET', markKey, 'claimed', 'overflow', 'fills', 'generation')
    -- Checked before the mark is consumed, so a settlement this script cannot
    -- act on leaves it for one that can.
    local usable = mark[1] ~= false
      and actual ~= nil and actual == actual and actual ~= math.huge and actual >= 0
      and tokensToAdd ~= nil and tokensToAdd > 0
      and maxTokens ~= nil and maxTokens > 0 and maxTokens ~= math.huge
      and interval ~= nil and interval == interval and interval ~= math.huge and interval ~= -math.huge

    if usable then
      redis.call('DEL', markKey)
      result.settled = true
      -- An absent bucket refilled entirely since the claim: nothing to correct.
      if redis.call('HEXISTS', key, 'tokens') == 1 then
        local tokens = finiteNumber(redis.call('HGET', key, 'tokens'), maxTokens)
        local lastUpdate = finiteNumber(redis.call('HGET', key, 'lastUpdate'), now)
        local overflow = finiteNumber(redis.call('HGET', key, 'overflow'), 0)
        local fills = finiteNumber(redis.call('HGET', key, 'fills'), 0)

${lazyRefill("        ")}

        local generation = finiteNumber(redis.call('HGET', key, 'generation'), 0)
        local claimed = finiteNumber(mark[1], 0)
        local overflowAtClaim = finiteNumber(mark[2], 0)
        local fillsAtClaim = finiteNumber(mark[3], 0)
        local generationAtClaim = finiteNumber(mark[4], 0)

        local applied = 0
        local unapplied = 0
        if generation == generationAtClaim and overflow >= overflowAtClaim and fills >= fillsAtClaim then
          local delta = actual - claimed
          if delta > 0 then
            local owed = delta - (overflow - overflowAtClaim)
            if owed < 0 then owed = 0 end
            local debt = 0
            if maxDebt ~= nil and maxDebt == maxDebt and maxDebt ~= math.huge and maxDebt > 0 then
              debt = maxDebt
            end
            local room = tokens + debt
            if room < 0 then room = 0 end
            applied = math.min(owed, room)
            unapplied = owed - applied
          elseif delta < 0 and fills == fillsAtClaim then
            local headroom = maxTokens - tokens
            if headroom < 0 then headroom = 0 end
            applied = -math.min(-delta, headroom)
          end
        end

        tokens = tokens - applied
        redis.call('HSET', key, 'tokens', tokens, 'lastUpdate', lastUpdate, 'overflow', overflow, 'fills', fills)
        redis.call('EXPIRE', key, bucketTtl(baseTtl, maxTokens, tokensToAdd, interval))
        result.applied = applied
        result.unapplied = unapplied
      end
    end
    results[i] = result
  end
  return cjson.encode(results)
`;
