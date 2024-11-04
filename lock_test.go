package redis_lock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTryLock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	lockClient := &Client{client: client}

	testCases := []struct {
		name       string
		key        string
		expiration time.Duration
		wantLock   *Lock
		wantErr    error
		before     func()
		after      func()
	}{
		{
			// 加锁成功
			name:       "locked",
			key:        "locked-key",
			expiration: 5 * time.Second,
			before:     func() {},
			after: func() {
				res, err := client.Del(ctx, "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			wantLock: &Lock{
				key: "locked-key",
			},
		},
		{
			// 模拟并发竞争失败
			name:       "failed",
			key:        "failed-key",
			expiration: 5 * time.Second,
			before: func() {
				// 模拟已经有人设置了分布式锁
				val, err := client.Set(ctx, "failed-key", "123", 5*time.Second).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", val)
			},
			after: func() {
				res, err := client.Del(ctx, "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			wantErr: ErrFailedToPreemptLock,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			l, err := lockClient.TryLock(ctx, tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.key, l.key) // 确保锁的值与预期相符
			assert.NotEmpty(t, l.value)    // 验证锁值不为空
			assert.NotEmpty(t, l.value)
			tc.after()
		})
	}
}
