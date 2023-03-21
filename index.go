package queue_redis

import (
	"github.com/infrago/infra"
	"github.com/infrago/queue"
)

func Driver() queue.Driver {
	return &redisDriver{}
}

func init() {
	infra.Register("redis", Driver())
}
