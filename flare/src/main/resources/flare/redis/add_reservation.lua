local executor = ARGV[1]
local stage_id = ARGV[2]
local attempt_id = ARGV[3]
local driver_id = ARGV[4]
local reservation_count = ARGV[5]
local reservation_pools = cjson.decode(ARGV[6])

local stage_name = "s"..stage_id.."_a"..attempt_id.."_d"..driver_id

redis.replicate_commands()

local add_pool = function(parent_path, pool)
    redis.call("SADD", "flare:pool:"..parent_path..":children", pool.name)
    redis.call("SADD", "flare:pool:"..parent_path..":executor_children:"..executor, pool.name)

    local pool_key = parent_path == "root" and "flare:pool:"..pool.name or "flare:pool:"..parent_path.."."..pool.name

    redis.call("HSETNX", pool_key, "running_tasks", 0)
    redis.call("HSETNX", pool_key, "pending_tasks", 0)

    if pool.max_share then
        redis.call("HSET", pool_key, "max_share", pool.max_share)
    end
    if pool.min_share then
        redis.call("HSET", pool_key, "min_share", pool.min_share)
    end
    if pool.weight then
        redis.call("HSET", pool_key, "weight", pool.weight)
    end
    if pool.is_stage then
        redis.call("HSET", pool_key, "is_stage", pool.is_stage)
    end
end

local add_reservation = function()
    local parent_path = "root"
    for i, pool in ipairs(reservation_pools) do
        add_pool(parent_path, pool)
        parent_path = (parent_path == "root" and pool.name or parent_path.."."..pool.name)
    end

    add_pool(parent_path, { name=stage_name, is_stage=1 })

    redis.call("HMSET", "flare:stage:"..stage_name, "stage_id", stage_id, "attempt_id", attempt_id, "driver_id", driver_id, "parent_pool", parent_path)
    redis.call("HINCRBY", "flare:stage:"..stage_name..":reservations", executor, reservation_count)

    return redis.status_reply("OK")
end

return add_reservation()