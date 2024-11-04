package redis_lock

import (
	"context"
	_ "embed"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	//go:embed script/lua/unlock.lua
	luaUnlock string
	//go:embed script/lua/refresh.lua
	luaRefresh string

	//go:embed script/lua/lock.lua
	luaLock string

	ErrFailedToPreemptLock = errors.New("redis-lock: 抢锁失败")
	// ErrLockNotHold 一般是出现在你预期你本来持有锁，结果却没有持有锁的地方
	// 比如说当你尝试释放锁的时候，可能得到这个错误
	// 这一般意味着有人绕开了 rlock 的控制，直接操作了 Redis
	ErrLockNotHold = errors.New("redis-lock: 未持有锁")
)

type Client struct {
	client redis.Cmdable
}

// TryLock 尝试获取锁
func (c *Client) TryLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	val := uuid.New().String() // 生成一个唯一的锁值
	// 尝试在Redis中以NX（如果不存在）方式设置锁，带有过期时间
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err // 返回错误信息
	}
	if !ok {
		// 如果ok为false，表示锁已经被其他请求持有
		return nil, ErrFailedToPreemptLock
	}
	// 返回一个新的Lock实例，表示成功获取锁
	return &Lock{
		client: c.client,
		key:    key,
		value:  val,
	}, nil
}

type Lock struct {
	client redis.Cmdable

	key   string
	value string // 锁的唯一标识
}

// Unlock 尝试释放锁
func (l *Lock) Unlock(ctx context.Context, key string) error {
	// 调用Lua脚本执行解锁操作，确保只有持有锁的客户端可以释放锁
	res, err := l.client.Eval(ctx, luaUnlock, []string{key}, l.value).Int64()
	if err != nil {
		return err // 返回执行错误
	}
	if res != 1 {
		// 如果返回值不是1，表示锁未被持有，抛出错误
		return ErrLockNotHold
	}
	return nil // 成功释放锁
}
