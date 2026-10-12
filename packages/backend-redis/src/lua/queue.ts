import {
  MAX_QUEUE_PRIORITY as MAX_PRIORITY,
  MAX_QUEUE_RETRIES as MAX_RETRIES,
  QUEUE_PRIORITY_BAND as PRIORITY_BAND,
  QUEUE_RETRY_BAND as RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS as SCORE_EPOCH_MS,
} from "@dianemo/core/backend";
import {
  exactFn,
  redisNow,
  claimLapsedFn,
  finiteNumberFn,
} from "./fragments.js";

/**
 * Request-queue Lua. Order is the sorted-set score packed by
 * `calculateQueueScore`; the metadata for each entry is a hash beside it.
 *
 * Twin of the memory backend's queue section.
 */

/**
 * Adds a request to the queue only if it is not already there.
 *
 * A retry re-enters with the same id and must keep its existing metadata.
 * Deciding that inside the script rather than reading the entry first saves a
 * round-trip and closes the race between the read and the write.
 */
export const addRequest = `
  local queueKey = KEYS[1]
  local metadataKey = KEYS[2]
  local tombstoneKey = KEYS[3]
  local requestId = ARGV[1]
  local score = ARGV[2]
  local ttl = ARGV[3]

  -- Refused after removal, which is what makes the ordering contract hold without
  -- depending on the transport: ioredis retries a NOSCRIPT as a NEW command one
  -- microtask later, so an abandonment published behind an outstanding add can be
  -- delivered first and remove an entry this script then creates. See
  -- docs/design-notes.md#a-removed-request-cannot-be-re-added.
  if redis.call('EXISTS', tombstoneKey) == 1 then
    return 0
  end

  if redis.call('EXISTS', metadataKey) == 1 then
    return 0
  end

  redis.call('ZADD', queueKey, score, requestId)
  local fields = {}
  for i = 4, #ARGV do
    table.insert(fields, ARGV[i])
  end
  redis.call('HSET', metadataKey, unpack(fields))
  redis.call('EXPIRE', queueKey, ttl)
  redis.call('EXPIRE', metadataKey, ttl)

  return 1
`;

export const removeRequest = `
  local queueKey = KEYS[1]
  local metadataKey = KEYS[2]
  local tombstoneKey = KEYS[3]
  local requestId = ARGV[1]
  local tombstoneTtl = ARGV[2]

  -- Read before deleting so the caller does not need a separate round-trip
  -- just to learn whether this was the thaw probe.
  local isThawRequest = redis.call('HGET', metadataKey, 'isThawRequest')

  redis.call('ZREM', queueKey, requestId)
  redis.call('DEL', metadataKey)
  -- Written even when there was nothing to remove: an abandonment that overtakes
  -- its own add finds no entry, and the marker is the only thing that stops the
  -- add from then creating one nobody awaits.
  redis.call('SET', tombstoneKey, '1', 'EX', tombstoneTtl)

  return isThawRequest or 'false'
`;

/**
 * Accepts an optional list of grantIds to skip (for frozen grants). A claim is
 * a lease: docs/design-notes.md#a-queue-claim-is-a-lease
 */
export const getNextRequest = `
  local queueKey = KEYS[1]
  local metadataKey = KEYS[2]
  local skipGrantIdsJson = ARGV[1] or '[]'
  local skipRequestIdsJson = ARGV[2] or '[]'

  -- Parse skip grantIds with error handling
  local skipGrantIds = {}
  if skipGrantIdsJson ~= '[]' then
    local ok, decoded = pcall(cjson.decode, skipGrantIdsJson)
    if ok and type(decoded) == 'table' then
      for _, grantId in ipairs(decoded) do
        skipGrantIds[grantId] = true
      end
    end
  end

  -- Request ids the caller has already judged unsatisfiable this pass.
  local skipRequestIds = {}
  if skipRequestIdsJson ~= '[]' then
    local ok, decoded = pcall(cjson.decode, skipRequestIdsJson)
    if ok and type(decoded) == 'table' then
      for _, requestId in ipairs(decoded) do
        skipRequestIds[requestId] = true
      end
    end
  end

${redisNow()}
${claimLapsedFn()}
  local allRequests = redis.call('ZRANGE', queueKey, 0, -1)

  for i, requestId in ipairs(allRequests) do
    local entryKey = metadataKey .. ':' .. requestId
    -- The rest of the entry is read only once it proves claimable: most of a
    -- deep queue is confirmed and in flight, and is passed over on these two.
    local state = redis.call('HMGET', entryKey, 'status', 'claimedAt')
    local status = state[1]
    local claimable = status == 'pending'
      or (status == 'inProgress' and claimLapsed(state[2]))
    if claimable and not skipRequestIds[requestId] then
      local fields = redis.call('HMGET', entryKey, 'grantId', 'requestId',
        'clientName', 'requestName')
      local grantId = fields[1] or ''
      -- What decodeRequest requires. Claiming an entry it then reads as null
      -- leaves the entry inProgress while the caller sees an empty queue. An
      -- empty requestName is a label like any other; HMGET's false is absence.
      local decodable = fields[2] and fields[2] ~= '' and fields[3]
        and fields[3] ~= '' and fields[4] ~= false
      if decodable and (grantId == '' or not skipGrantIds[grantId]) then
        -- A lapsed claim's probe nomination lapses with it: left set, it holds
        -- off every other probe for a request nobody is admitting.
        if status ~= 'pending' then
          redis.call('HSET', entryKey, 'isThawRequest', 'false')
        end
        redis.call('HSET', entryKey, 'status', 'inProgress',
          'claimedAt', string.format('%.0f', now))
        return cjson.encode(redis.call('HGETALL', entryKey))
      end
    end
  end

  return nil
`;

/** Twin of the memory backend's `confirmRequest`. */
export const confirmRequest = `
  local metadataKey = KEYS[1]
  local claimedAt = ARGV[1]
${redisNow()}
${claimLapsedFn()}

  local fields = redis.call('HMGET', metadataKey, 'status', 'claimedAt')
  if fields[1] ~= 'inProgress' then return 0 end
  -- The nil test because nil equals nil, where the memory twin's NaN equals
  -- nothing: an absent stamp confirms no claim.
  local stored = tonumber(fields[2])
  if stored == nil or stored ~= tonumber(claimedAt) then return 0 end
  -- A lapsed claim is refused even when nobody has taken it over: its probe
  -- mark no longer counts, so confirming it could dispatch a second probe.
  if claimLapsed(fields[2]) then return 0 end
  redis.call('HDEL', metadataKey, 'claimedAt')
  return 1
`;

/**
 * Updates metadata fields and recalculates the queue score if priority or retries changed.
 */
export const updateRequest = `
  local queueKey = KEYS[1]
  local metadataKey = KEYS[2]
  local updatesJson = ARGV[1]

  -- Check if request exists
  local exists = redis.call('EXISTS', metadataKey)
  if exists == 0 then
    return 0
  end

  -- Parse updates with error handling
  local ok, updates = pcall(cjson.decode, updatesJson)
  if not ok then
    return 0
  end
  -- 'type(x) == number', not 'x ~= nil': cjson decodes a JSON null to a
  -- lightuserdata that is not nil, and JSON.stringify writes null for both NaN and
  -- Infinity, so a non-finite update is skipped rather than stored.
  local needsScoreUpdate =
    type(updates.retries) == 'number' or type(updates.priority) == 'number'

  -- Resolved before the first HSET, because the re-score is not optional: the
  -- entry that carries the new priority and the sorted-set member that orders it
  -- must change together or not at all, and there is no member to re-score
  -- without this field.
  local requestId = nil
  if needsScoreUpdate then
    requestId = redis.call('HGET', metadataKey, 'requestId')
    if not requestId then return 0 end
  end

  if updates.status ~= nil then
    redis.call('HSET', metadataKey, 'status', updates.status)
    redis.call('HDEL', metadataKey, 'claimedAt')
  end
  if type(updates.retries) == 'number' then
    redis.call('HSET', metadataKey, 'retries', tostring(updates.retries))
  end
  if type(updates.priority) == 'number' then
    redis.call('HSET', metadataKey, 'priority', tostring(updates.priority))
  end
  if updates.isThawRequest ~= nil then
    -- Store as 'true' or 'false' string to match addRequest behavior
    local value = 'false'
    if updates.isThawRequest then
      value = 'true'
    end
    redis.call('HSET', metadataKey, 'isThawRequest', value)
  end

  -- Packs the score exactly as calculateQueueScore does, from the same
  -- constants, so a re-scored entry sorts on the same scale as a fresh one.
  if needsScoreUpdate then
    local priority = tonumber(redis.call('HGET', metadataKey, 'priority')) or 1
    local retries = tonumber(redis.call('HGET', metadataKey, 'retries')) or 0
    local timestamp = tonumber(redis.call('HGET', metadataKey, 'timestamp')) or 0

    if priority ~= priority then priority = 1 end
    if retries ~= retries then retries = 0 end
    if timestamp ~= timestamp then timestamp = 0 end

    if priority < 0 then priority = 0 end
    if priority > ${MAX_PRIORITY} then priority = ${MAX_PRIORITY} end
    if retries < 0 then retries = 0 end
    if retries > ${MAX_RETRIES} then retries = ${MAX_RETRIES} end

    -- The arrival term is clamped into its own band, which calculateQueueScore
    -- instead enforces with a RangeError. A stored timestamp is not a constant a
    -- maintainer chose, so a corrupt one is clamped rather than raised: a raise
    -- here would fail the retry it is re-scoring.
    local arrival = timestamp - ${SCORE_EPOCH_MS}
    if arrival < 0 then arrival = 0 end
    if arrival > ${RETRY_BAND} - 1 then arrival = ${RETRY_BAND} - 1 end

    local score = (${MAX_PRIORITY} - priority) * ${PRIORITY_BAND}
      + (${MAX_RETRIES} - retries) * ${RETRY_BAND}
      + arrival
    redis.call('ZADD', queueKey, score, requestId)
  end

  return 1
`;

export const getQueueStats = `
  local queueKey = KEYS[1]
  local metadataKeyPrefix = KEYS[2]

  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)
${finiteNumberFn()}
${exactFn()}
  local pending = 0
  local inProgress = 0
  local totalCost = 0
  local costByLimit = {}

  for i, requestId in ipairs(requestIds) do
    local metadataKey = metadataKeyPrefix .. ':' .. requestId
    local fields = redis.call('HMGET', metadataKey, 'status', 'cost')
    local status = fields[1]
    local raw = fields[2]

    -- '~= ""' as well as the false test: HMGET yields false for an absent field,
    -- but an empty string is truthy in Lua and falsy in the memory twin's
    -- JavaScript, so a blank status counted its cost here and was skipped there.
    if status and status ~= '' then
      if type(raw) == 'string' and string.match(raw, '^%s*{') then
        -- A per-limit cost, stored as JSON. pcall and the finite test, because
        -- one corrupt entry must not fail the stats read for the whole queue.
        local ok, decoded = pcall(cjson.decode, raw)
        if ok and type(decoded) == 'table' then
          for name, amount in pairs(decoded) do
            if type(name) == 'string' and type(amount) == 'number'
              and amount == amount and amount ~= math.huge and amount ~= -math.huge then
              costByLimit[name] = (costByLimit[name] or 0) + amount
            end
          end
        end
      else
        -- finiteNumber: a stored 'nan' or '1e400' parses through tonumber and
        -- then fails in cjson.encode below, which failed the stats read for every
        -- request in the queue rather than the one corrupt entry.
        totalCost = totalCost + finiteNumber(raw, 1)
      end
      if status == 'pending' then
        pending = pending + 1
      elseif status == 'inProgress' then
        inProgress = inProgress + 1
      end
    end
  end

  -- Sent as exact strings like totalCost: a sum of finite entries can still
  -- overflow to inf, which cjson refuses to encode.
  local exactByLimit = {}
  for name, total in pairs(costByLimit) do exactByLimit[name] = exact(total) end
  return cjson.encode({ pending = pending, inProgress = inProgress, totalCost = exact(totalCost), costByLimit = exactByLimit })
`;

export const getAllRequests = `
  local queueKey = KEYS[1]
  local metadataKeyPrefix = KEYS[2]

  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)
  local requests = {}

  for i, requestId in ipairs(requestIds) do
    local metadataKey = metadataKeyPrefix .. ':' .. requestId
    local metadata = redis.call('HGETALL', metadataKey)
    if #metadata > 0 then
      table.insert(requests, metadata)
    end
  end

  -- cjson encodes an empty Lua table as '{}', not '[]', so an empty queue
  -- would come back as an object the caller cannot iterate.
  if #requests == 0 then
    return '[]'
  end
  return cjson.encode(requests)
`;

/**
 * Removes requests that:
 * - Have no metadata (expired TTL)
 * - Are owned by an instance not in the alive set
 *
 * Both bails below leave the queue untouched. See the memory backend's twin.
 */
export const cleanupOrphanedRequests = `
  local queueKey = KEYS[1]
  local metadataKeyPrefix = KEYS[2]
  local aliveIdsJson = ARGV[1]
  local currentInstanceId = ARGV[2]

  -- An argument that does not decode says nothing about who is alive, and
  -- carrying on with an empty set reaps every entry in the queue.
  local ok, decoded = pcall(cjson.decode, aliveIdsJson)
  if not ok or type(decoded) ~= 'table' then return 0 end

  -- Parse alive instance IDs into a set for O(1) lookup
  local aliveIds = {}
  for _, id in ipairs(decoded) do
    aliveIds[id] = true
  end

  -- Every instance re-adds its own id on each heartbeat, so a set without the
  -- sweeper's own is a read that was lost rather than a fleet that died.
  if not aliveIds[currentInstanceId] then return 0 end

  -- Get all request IDs from the queue
  local requestIds = redis.call('ZRANGE', queueKey, 0, -1)
  local cleanedUp = 0

  for i, requestId in ipairs(requestIds) do
    local metadataKey = metadataKeyPrefix .. ':' .. requestId

    -- Check if metadata exists
    local ownerId = redis.call('HGET', metadataKey, 'ownerId')

    if ownerId == false or ownerId == nil then
      -- Metadata doesn't exist (expired TTL), remove from queue only
      redis.call('ZREM', queueKey, requestId)
      cleanedUp = cleanedUp + 1
    elseif ownerId == '' or not aliveIds[ownerId] then
      -- Owned by dead instance, remove from queue and delete metadata
      redis.call('ZREM', queueKey, requestId)
      redis.call('DEL', metadataKey)
      cleanedUp = cleanedUp + 1
    end
  end

  return cleanedUp
`;
