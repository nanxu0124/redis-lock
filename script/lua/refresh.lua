-- 获取当前锁值
local currentLockValue = redis.call("get", KEYS[1])

-- 如果锁值与预期的相同，刷新过期时间
if currentLockValue == ARGV[1] then
    -- 设置新过期时间，返回1表示成功
    return redis.call("expire", KEYS[1], ARGV[2])
else
    -- 锁值不同，返回0表示刷新失败
    return 0
end
