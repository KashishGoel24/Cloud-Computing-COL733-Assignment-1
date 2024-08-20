#!lua name=mylib

local function atomicIncrementAndAck(keys, args)
    local ack = redis.call('xack', keys[1], args[1], args[2])
    if ack == 1 then
        for i = 3, #args, 2 do
            redis.call('zincrby', keys[2], args[i + 1], args[i])
        end
        return ack
    else
        return 0
    end
end
redis.register_function('atomicIncrementAndAck', atomicIncrementAndAck)