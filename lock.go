package redis_lock

import (
	"context"
	_ "embed"
	"errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
	"time"
)

var (
	//go:embed script/lua/unlock.lua
	luaUnlock string
	//go:embed script/lua/refresh.lua
	luaRefresh string

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
		client:     c.client,
		key:        key,
		value:      val,
		expiration: expiration,
		unlock:     make(chan struct{}, 1),
	}, nil
}

// Lock 表示一个 Redis 分布式锁，封装了 Redis 客户端、锁的关键信息、锁的释放机制等。
// - client: Redis 客户端接口，用于执行 Redis 操作。
// - key: 锁的唯一标识符，用于在 Redis 中标识该锁。
// - value: 锁的值，通常是一个唯一标识符，用于确保锁的唯一性。
// - expiration: 锁的过期时间，超过此时间后锁将自动失效。
// - unlock: 用于接收释放锁的信号。通过该通道通知其他操作或结束自动刷新等任务。
// - unlockOnce: 用于确保释放锁的操作只执行一次。
type Lock struct {
	client redis.Cmdable

	key        string
	value      string // 锁的唯一标识
	expiration time.Duration

	unlock     chan struct{}
	unlockOnce sync.Once
}

// AutoRefresh 自动刷新锁，确保锁的有效期不会过期。
// - internal: 刷新锁的时间间隔（每隔多久刷新一次锁）
// - timeout: 刷新锁时的超时时间，防止操作被阻塞太久
// - retryMax: 刷新失败时的最大重试次数，防止死循环
func (l *Lock) AutoRefresh(ctx context.Context, internal time.Duration, timeout time.Duration, retryMax int) error {
	// 定时器，按照 internal 的时间间隔触发刷新操作
	ticker := time.NewTicker(internal)
	defer ticker.Stop()

	// 用于重试机制的信号通道，确保在失败时可以尝试重新刷新锁
	retrySig := make(chan struct{}, 1)
	retryCnt := 0
	for {
		select {
		case <-ticker.C:
			// 每隔 internal 时间间隔执行一次刷新操作
			// 创建带有超时控制的子上下文
			rctx, cancel := context.WithTimeout(ctx, timeout)
			err := l.Refresh(rctx)
			cancel()
			// 如果刷新操作超时，发送信号并继续等待下次刷新
			if errors.Is(err, context.DeadlineExceeded) {
				retrySig <- struct{}{}
				continue
			}
			if err != nil {
				log.Println("redis lock autorefresh failed:", err)
				return err
			}

		case <-retrySig:
			// 触发重试机制
			retryCnt++
			if retryCnt > retryMax {
				return errors.New("redis lock autorefresh failed: too many retries")
			} else {
				rctx, cancel := context.WithTimeout(ctx, timeout)
				err := l.Refresh(rctx)
				cancel()
				if errors.Is(err, context.DeadlineExceeded) {
					retrySig <- struct{}{}
					continue
				}
				if err != nil {
					log.Println("redis lock autorefresh failed:", err)
					return err
				}
			}
			retryCnt = 0

		case <-l.unlock:
			// 如果接收到锁释放的信号（通过 l.unlock 通道），则退出自动刷新循环
			// 这个通道可用于通知锁已释放，停止自动刷新操作
			return nil
		}
	}
}

// Refresh 刷新锁的过期时间。
// 通过执行 Redis Lua 脚本来刷新锁的过期时间。
// 如果锁不再持有，返回 ErrLockNotHold 错误。
func (l *Lock) Refresh(ctx context.Context) error {
	// 调用 Redis Lua 脚本，尝试刷新锁的过期时间
	// Lua 脚本的参数：锁的 key、当前锁值、过期时间（秒）
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.value, l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}
	// 如果返回的结果不是 1，表示锁不再持有，返回锁未持有错误
	if res != 1 {
		return ErrLockNotHold
	}
	return nil
}

// Unlock 尝试释放锁。
// 通过 Lua 脚本确保只有持有该锁的客户端才能释放锁，避免非持有者误操作。
// 如果调用者不是该锁的持有者，解锁操作将失败。
// 如果解锁成功，会通过 unlock 通道通知相关操作停止（例如自动刷新）。
//
// 参数:
// - ctx: 上下文，用于控制操作的超时等。
// - key: 锁的唯一标识符，对应 Redis 中的 key。
//
// 返回:
// - 如果成功释放锁，返回 nil。
// - 如果锁未被持有，返回 ErrLockNotHold 错误。
// - 如果出现 Redis 操作错误，返回相应的错误。
func (l *Lock) Unlock(ctx context.Context, key string) error {
	// 确保锁的释放操作只会执行一次（通过 unlockOnce 保证）
	l.unlockOnce.Do(func() {
		// 发送信号到 unlock 通道，通知相关操作（如自动刷新）停止
		l.unlock <- struct{}{}
	})

	// 调用 Lua 脚本尝试释放锁
	// Lua 脚本检查锁的持有者是否与当前客户端一致，并执行删除锁的操作
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
