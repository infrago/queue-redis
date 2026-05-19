package queue_redis

import (
	"context"
	"net"
	"strconv"
	"strings"
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
			Addr:     redisAddr(setting),
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

	recovered := make(map[string]struct{}, len(c.queues))
	for _, queueName := range c.queues {
		if _, ok := recovered[queueName]; ok {
			continue
		}
		if err := c.recoverProcessing(queueName); err != nil {
			return err
		}
		recovered[queueName] = struct{}{}
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

				processing := processingName(q)
				raw, err := c.client.BRPopLPush(context.Background(), q, processing, time.Second).Result()
				if err != nil {
					if err == redis.Nil {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				msg := redisMessage{}
				if err := infra.Unmarshal(infra.JSON, []byte(raw), &msg); err != nil {
					c.ack(processing, raw)
					continue
				}
				now := time.Now()
				if msg.After > 0 {
					after := time.Unix(0, msg.After)
					if after.After(now) {
						delay := time.Until(after)
						if delay > time.Second {
							delay = time.Second
						}
						timer := time.NewTimer(delay)
						select {
						case <-timer.C:
						case <-c.done:
							timer.Stop()
							return
						}
						msg.After = after.UnixNano()
						if c.publish(q, &msg) == nil {
							c.ack(processing, raw)
						}
						continue
					}
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
					msg.After = now.Add(res.Delay).UnixNano()
					if c.publish(q, &msg) == nil {
						c.ack(processing, raw)
					}
				} else {
					c.ack(processing, raw)
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

func (c *redisConnection) ack(processing, raw string) {
	_ = c.client.LRem(context.Background(), processing, 1, raw).Err()
}

func (c *redisConnection) recoverProcessing(name string) error {
	processing := processingName(name)
	for {
		_, err := c.client.RPopLPush(context.Background(), processing, name).Result()
		if err == redis.Nil {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func processingName(name string) string {
	return name + ":processing"
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
		After:   time.Now().Add(delay).UnixNano(),
		Data:    data,
	})
}

func redisAddr(setting map[string]any) string {
	if v, ok := setting["addr"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}

	host := ""
	if v, ok := setting["server"].(string); ok && strings.TrimSpace(v) != "" {
		host = strings.TrimSpace(v)
	}
	if v, ok := setting["host"].(string); ok && strings.TrimSpace(v) != "" {
		host = strings.TrimSpace(v)
	}
	if host == "" {
		host = "127.0.0.1"
	}

	port := "6379"
	if v, ok := setting["port"].(string); ok && strings.TrimSpace(v) != "" {
		port = strings.TrimSpace(v)
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(host, port)
}

var _ queue.Connection = (*redisConnection)(nil)
