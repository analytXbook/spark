local executor = ARGV[1]
local stage_id = ARGV[2]
local attempt_id = ARGV[3]
local driver_id = ARGV[4]

local stage_name = "s"..stage_id.."_a"..attempt_id.."_d"..driver_id

redis.replicate_commands()

local function add_running_task(pool_path)
  redis.call("HINCRBY", "flare:pool:"..pool_path, "pending_tasks", -1)
  redis.call("HINCRBY", "flare:pool:"..pool_path, "running_tasks", 1)

  if pool_path == "root" then
    return true
  end

  local last_pool_index = pool_path:find("%.[^%.]*$")
  local parent_path = last_pool_index and pool_path:sub(1, last_pool_index - 1) or "root"

  return add_running_task(parent_path)
end

local task_launched = function()
  local stage_key = "flare:stage:"..stage_name
  local parent_path = redis.call("HGET", stage_key, "parent_pool")

  if parent_path then
    local stage_pool_path = parent_path.."."..stage_name

    add_running_task(stage_pool_path)
  end

  return redis.status_reply("OK")
end

return task_launched()