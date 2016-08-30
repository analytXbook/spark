local executor = ARGV[1]
local stage_id = ARGV[2]
local attempt_id = ARGV[3]
local driver_id = ARGV[4]

local stage_name = "s"..stage_id.."_a"..attempt_id.."_d"..driver_id

redis.replicate_commands()

local srem_and_is_empty = function(key, member)
    redis.call("SREM", key, member)
    return redis.call("SCARD", key) == 0
end

local hdel_and_is_empty = function(key, field)
    redis.call("HDEL", key, field)
    return redis.call("HLEN", key) == 0
end

local function cleanup_pool(pool_path, child, child_deleted)
    local pool_key = "flare:pool:"..pool_path
    local pool_deleted = false

    if child_deleted and srem_and_is_empty(pool_key..":children", child) then
        redis.call("DEL", pool_key)
        pool_deleted = true
    end

    if srem_and_is_empty(pool_key..":executor_children:"..executor, child) then
        redis.call("SREM", pool_key..":executors", executor)
        
        if pool_path == "root" then
          return true
        end
        
        local last_pool_index = pool_path:find("%.[^%.]*$")

        local parent_path, pool_name
      
        if (last_pool_index) then
            parent_path = pool_path:sub(1, last_pool_index - 1)
            pool_name = pool_path:sub(last_pool_index + 1)
        else
            parent_path = "root"
            pool_name = pool_path
        end

        return cleanup_pool(parent_path, pool_name, pool_deleted)
    end
end

local remove_reservation = function()
    local stage_key = "flare:stage:"..stage_name
    local parent_path = redis.call("HGET", stage_key, "parent_pool")

    local stage_finished = false

    if parent_path then
        if hdel_and_is_empty(stage_key..":reservations", executor) then
            local stage_pool_path = parent_path ~= "root" and parent_path.."."..stage_name or stage_name
            redis.call("DEL", "flare:pool:"..stage_pool_path)

            redis.call("DEL", stage_key)

            stage_finished = true
        end

        cleanup_pool(parent_path, stage_name, stage_finished)
    end

    return stage_finished
end

return remove_reservation()