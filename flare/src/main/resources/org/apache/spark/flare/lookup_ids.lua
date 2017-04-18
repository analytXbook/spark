local id_group = ARGV[1]
local range_end = ARGV[2]

local lookup_id_range = function()
    local driver_id = redis.call("HGET", "flare:id:"..id_group, range_end)
    return tonumber(driver_id)
end

return lookup_id_range()