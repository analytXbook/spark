local executor = ARGV[1]

redis.replicate_commands()

local get_pool = function(parent_path, name)
	local pool_path = parent_path == "root" and name or parent_path.."."..name  
  local pool_key = "flare:pool:"..pool_path
  
  local pool = { name = name, parent = parent_path }

  pool.min_share = tonumber(redis.call("HGET", pool_key, "min_share"))
  pool.max_share = tonumber(redis.call("HGET", pool_key, "max_share"))
  pool.weight = tonumber(redis.call("HGET", pool_key, "weight"))
  pool.pending_tasks = tonumber(redis.call("HGET", pool_key, "pending_tasks"))
  pool.running_tasks = tonumber(redis.call("HGET", pool_key, "running_tasks"))
  pool.is_stage = redis.call("HGET", pool_key, "is_stage")
    
	return pool
end

local get_stage = function(name)
    local stage_key = "flare:stage:"..name
    
    local stage = { name = name }
    
    stage.reservations = tonumber(redis.call("HGET", stage_key..":reservations", executor))
    stage.parent_pool = redis.call("HGET", stage_key, "parent_pool")
    stage.stage_id = redis.call("HGET", stage_key, "stage_id")
    stage.attempt_id = redis.call("HGET", stage_key, "attempt_id")
    stage.driver_id = redis.call("HGET", stage_key, "driver_id")
  
    return stage
end

local fair_comparator = function(p1, p2)
	local p1_active_tasks = (p1.running_tasks or 0) + (p1.pending_tasks or 0)
	local p2_active_tasks = (p1.running_tasks or 0) + (p2.pending_tasks or 0)

	local p1_maxed = p1.max_share ~= nil and (p1.max_share > p1_active_tasks)
	local p2_maxed = p2.max_share ~= nil and (p2.max_share > p2_active_tasks)
	
	if p1_maxed and p2_maxed then
		return p1.name < p1.name
	elseif p1_maxed and not p2_maxed then	
		return false
	elseif not p1_maxed and p2_maxed then
		return true	
	end
	
	local p1_min_share = p1.min_share or 0
	local p2_min_share = p2.min_share or 0
	
	local p1_needy = p1_active_tasks < p1_min_share
	local p2_needy = p2_active_tasks < p2_min_share
	
	local compare
	
	if p1_needy and not p2_needy then
		return true
	elseif not p1_needy and p2_needy then
		return false
	elseif p1_needy and p2_needy then
		compare = (p1_active_tasks / math.max(p1_min_share, 1)) - (p2_active_tasks / math.max(p2_min_share, 1))
	else
		compare = (p1_active_tasks * (p1.weight or 1)) - (p2_active_tasks * (p2.weight or 1))
	end

	if compare < 0 then
		return true
	elseif compare > 0 then
		return false
	else
		return p1.name < p2.name 
	end
end

local function add_pending_task(pool_path)
	redis.call("HINCRBY", "flare:pool:"..pool_path, "pending_tasks", 1)

	if pool_path == "root" then
		return true
	end

	local last_pool_index = pool_path:find("%.[^%.]*$")
	local parent_path = last_pool_index and pool_path:sub(1, last_pool_index - 1) or "root"

	return add_pending_task(parent_path)
end

local function next_reservation(parent_path)
	local child_pools = redis.call("SINTER", "flare:pool:"..parent_path..":children" , "flare:pool:"..parent_path..":executor_children:"..executor)
		
	for i, child_name in ipairs(child_pools) do
		child_pools[i] = get_pool(parent_path, child_name)
	end

	table.sort(child_pools, fair_comparator)

	for i, pool in ipairs(child_pools) do
    local pool_path = parent_path == "root" and pool.name or parent_path.."."..pool.name
		if pool.is_stage then
      local stage = get_stage(pool.name)
			if stage.reservations > 0 then
				redis.call("HINCRBY", "flare:stage:"..pool.name..":reservations", executor, -1)
				add_pending_task(pool_path)
				return {stage.stage_id, stage.attempt_id, stage.driver_id}
      end
    elseif pool.max_share and pool.max_share > pool.running_tasks or true then
			  return next_reservation(pool_path)
    end
  end
	
	return false
end

return next_reservation("root")