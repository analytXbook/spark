local driver_id = ARGV[1]
local id_group = ARGV[2]
local is_int = (ARGV[3] == "true")
local range_size = tonumber(ARGV[4])

redis.replicate_commands()

local allocate_range = function()
   local range_start = tonumber(redis.call("GET", "flare:id:"..id_group..":position")) or 0
   local range_end = range_start + range_size - 1

   if (is_int and range_end > 2147483647) then
     range_end = 2147483647
   end

   redis.call("HSET", "flare:id:"..id_group, range_end, driver_id)
   redis.call("SET", "flare:id:"..id_group..":position", range_end + 1)

   return { range_start, range_end }
end

return allocate_range()

