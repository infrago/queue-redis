package queue_redis

import (
	"github.com/infrago/queue"
)

func Driver() queue.Driver {
	return &redisDriver{}
}

func init() {
	queue.Register("redis", Driver())
}
