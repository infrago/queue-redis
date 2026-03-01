package queue_redis

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/infrago/infra"
	"github.com/infrago/queue"
	"github.com/redis/go-redis/v9"
)

func init() {
	infra.Register("redis", &redisDriver{})
}

type (
	redisDriver struct{}

	redisConnection struct {
		mutex    sync.RWMutex
		running  bool
		client   *redis.Client
		instance *queue.Instance
		queues   []string
		done     chan struct{}
		wg       sync.WaitGroup
	}

	redisMessage struct {
		Attempt int    `json:"attempt"`
		After   int64  `json:"after"`
		Data    []byte `json:"data"`
	}
)

func (d *redisDriver) Connect(inst *queue.Instance) (queue.Connection, error) {
	setting := inst.Config.Setting

	addr := "127.0.0.1:6379"
	host := ""
	port := "6379"
	if v, ok := setting["port"].(string); ok && v != "" {
		port = v
	}
	if v, ok := setting["server"].(string); ok && v != "" {
		host = v
	}
	if v, ok := setting["host"].(string); ok && v != "" {
		host = v
	}
	if host != "" {
		addr = host + ":" + port
	}
	if v, ok := setting["addr"].(string); ok && v != "" {
		addr = v
	}

	username, _ := setting["username"].(string)
	password, _ := setting["password"].(string)

	database := 0
	switch v := setting["database"].(type) {
	case int:
		database = v
	case int64:
		database = int(v)
	case float64:
		database = int(v)
	case string:
		if vv, err := strconv.Atoi(v); err == nil {
			database = vv
		}
	}

	return &redisConnection{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Username: username,
			Password: password,
			DB:       database,
		}),
		instance: inst,
		queues:   make([]string, 0),
		done:     make(chan struct{}),
	}, nil
}

func (c *redisConnection) Open() error {
	return c.client.Ping(context.Background()).Err()
}

func (c *redisConnection) Close() error {
	return c.client.Close()
}

func (c *redisConnection) Register(name string) error {
	c.mutex.Lock()
	c.queues = append(c.queues, name)
	c.mutex.Unlock()
	return nil
}

func (c *redisConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for _, queueName := range c.queues {
		q := queueName
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.done:
					return
				default:
				}

				values, err := c.client.BRPop(context.Background(), time.Second, q).Result()
				if err != nil {
					if err == redis.Nil {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if len(values) < 2 {
					continue
				}

				msg := redisMessage{}
				if err := infra.Unmarshal(infra.JSON, []byte(values[1]), &msg); err != nil {
					continue
				}
				now := time.Now()
				if msg.After > 0 && msg.After > now.Unix() {
					time.Sleep(time.Second)
					c.publish(q, &msg)
					continue
				}

				req := queue.Request{
					Name:      q,
					Data:      msg.Data,
					Attempt:   msg.Attempt,
					Timestamp: now,
				}
				res := c.instance.Serve(req)
				if res.Retry {
					msg.Attempt++
					msg.After = now.Add(res.Delay).Unix()
					c.publish(q, &msg)
				}
			}
		}()
	}
	c.running = true
	return nil
}

func (c *redisConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}
	close(c.done)
	c.wg.Wait()
	c.done = make(chan struct{})
	c.running = false
	return nil
}

func (c *redisConnection) publish(name string, msg *redisMessage) error {
	data, err := infra.Marshal(infra.JSON, msg)
	if err != nil {
		return err
	}
	return c.client.LPush(context.Background(), name, data).Err()
}

func (c *redisConnection) Publish(name string, data []byte) error {
	return c.publish(name, &redisMessage{
		Attempt: 1,
		After:   0,
		Data:    data,
	})
}

func (c *redisConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	return c.publish(name, &redisMessage{
		Attempt: 1,
		After:   time.Now().Add(delay).Unix(),
		Data:    data,
	})
}

var _ queue.Connection = (*redisConnection)(nil)
