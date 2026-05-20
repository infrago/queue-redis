package queue_redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
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
		mutex     sync.RWMutex
		running   bool
		client    *redis.Client
		instance  *queue.Instance
		queues    map[string]int
		group     string
		consumer  string
		claimIdle time.Duration
		idleClean time.Duration
		maxLen    int64
		consumers map[string][]string
		done      chan struct{}
		wg        sync.WaitGroup
	}

	redisMessage struct {
		Attempt int    `json:"attempt"`
		After   int64  `json:"after"`
		Data    []byte `json:"data"`
	}

	redisDelayedMessage struct {
		ID      string `json:"id"`
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

	group := stringSetting(setting, "group", "infragoq")
	consumer := stringSetting(setting, "consumer", redisConsumerName())
	claim := durationSetting(setting, "claim", 5*time.Minute)
	if claim <= 0 {
		claim = 5 * time.Minute
	}
	idleClean := durationSetting(setting, "consumer_idle", time.Hour)
	maxLen := int64Setting(setting, "maxlen", 0)

	return &redisConnection{
		client: redis.NewClient(&redis.Options{
			Addr:     redisAddr(setting),
			Username: username,
			Password: password,
			DB:       database,
		}),
		instance:  inst,
		queues:    make(map[string]int, 0),
		group:     group,
		consumer:  consumer,
		claimIdle: claim,
		idleClean: idleClean,
		maxLen:    maxLen,
		consumers: make(map[string][]string, 0),
		done:      make(chan struct{}),
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
	c.queues[name]++
	c.mutex.Unlock()
	return nil
}

func (c *redisConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for queueName := range c.queues {
		if err := c.ensureGroup(queueName); err != nil {
			return err
		}
		c.cleanupIdleConsumers(queueName)
	}

	for queueName, workers := range c.queues {
		q := queueName
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.moveDelayed(q)
		}()
		for i := 0; i < workers; i++ {
			consumer := fmt.Sprintf("%s-%s-%d", c.consumer, streamToken(q), i)
			c.consumers[q] = append(c.consumers[q], consumer)
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.consume(q, consumer)
			}()
		}
	}
	if c.idleClean > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.cleanConsumers()
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
	for name, consumers := range c.consumers {
		stream := redisStreamName(name)
		for _, consumer := range consumers {
			if err := c.client.XGroupDelConsumer(context.Background(), stream, c.group, consumer).Err(); err != nil {
				logRedisError("delete consumer", name, err)
			}
		}
	}
	c.consumers = make(map[string][]string, 0)
	c.done = make(chan struct{})
	c.running = false
	return nil
}

func (c *redisConnection) publish(name string, msg *redisMessage) error {
	args := &redis.XAddArgs{
		Stream: redisStreamName(name),
		Values: redisMessageValues(msg),
	}
	if c.maxLen > 0 {
		args.MaxLen = c.maxLen
		args.Approx = true
	}
	return c.client.XAdd(context.Background(), args).Err()
}

func (c *redisConnection) delay(name string, msg *redisMessage) error {
	member, err := redisDelayedMember(msg)
	if err != nil {
		return err
	}
	score := float64(time.Unix(0, msg.After).UnixMilli())
	return c.client.ZAdd(context.Background(), redisDelayName(name), redis.Z{
		Score:  score,
		Member: member,
	}).Err()
}

func (c *redisConnection) ack(stream, id string) {
	ctx := context.Background()
	if err := c.client.XAck(ctx, stream, c.group, id).Err(); err != nil {
		logRedisError("ack", stream, err)
		return
	}
	if err := c.client.XDel(ctx, stream, id).Err(); err != nil {
		logRedisError("delete", stream, err)
	}
}

func (c *redisConnection) retry(stream, id, name string, msg *redisMessage) error {
	if msg.After > time.Now().UnixNano() {
		member, err := redisDelayedMember(msg)
		if err != nil {
			return err
		}
		return redisDelayAckScript.Run(context.Background(), c.client, []string{stream, redisDelayName(name)}, c.group, id, time.Unix(0, msg.After).UnixMilli(), member).Err()
	}
	values := redisMessageValues(msg)
	return redisStreamAckScript.Run(context.Background(), c.client, []string{stream, redisStreamName(name)}, c.group, id, values["attempt"], values["after"], values["data"], c.maxLen).Err()
}

func (c *redisConnection) ensureGroup(name string) error {
	if err := c.migrateLegacyLists(name); err != nil {
		return err
	}
	stream := redisStreamName(name)
	err := c.client.XGroupCreateMkStream(context.Background(), stream, c.group, "0").Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (c *redisConnection) consume(name, consumer string) {
	stream := redisStreamName(name)
	for {
		select {
		case <-c.done:
			return
		default:
		}

		if ok := c.claim(name, consumer); ok {
			continue
		}

		streams, err := c.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    time.Second,
		}).Result()
		if err != nil {
			if err != redis.Nil && !errors.Is(err, context.Canceled) {
				logRedisError("read", name, err)
				sleepOrDone(c.done, 100*time.Millisecond)
			}
			continue
		}
		c.handleStreams(name, streams)
	}
}

func (c *redisConnection) claim(name, consumer string) bool {
	stream := redisStreamName(name)
	msgs, _, err := c.client.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    c.group,
		Consumer: consumer,
		MinIdle:  c.claimIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			logRedisError("claim", name, err)
		}
		return false
	}
	if len(msgs) == 0 {
		return false
	}
	c.handleMessages(name, msgs)
	return true
}

func (c *redisConnection) handleStreams(name string, streams []redis.XStream) {
	for _, stream := range streams {
		c.handleMessages(name, stream.Messages)
	}
}

func (c *redisConnection) handleMessages(name string, messages []redis.XMessage) {
	stream := redisStreamName(name)
	for _, raw := range messages {
		msg, err := redisStreamMessage(raw)
		if err != nil {
			logRedisError("decode", name, err)
			c.ack(stream, raw.ID)
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
				if !sleepOrDone(c.done, delay) {
					return
				}
				if err := c.publish(name, msg); err != nil {
					logRedisError("defer", name, err)
					continue
				}
				c.ack(stream, raw.ID)
				continue
			}
		}

		req := queue.Request{
			Name:      name,
			Data:      msg.Data,
			Attempt:   msg.Attempt,
			Timestamp: now,
		}
		res := c.instance.Serve(req)
		if res.Retry {
			msg.Attempt++
			msg.After = now.Add(res.Delay).UnixNano()
			if err := c.retry(stream, raw.ID, name, msg); err != nil {
				logRedisError("retry", name, err)
				continue
			}
			continue
		}
		c.ack(stream, raw.ID)
	}
}

func (c *redisConnection) moveDelayed(name string) {
	bucket := redisDelayName(name)
	for {
		select {
		case <-c.done:
			return
		default:
		}

		now := strconv.FormatInt(time.Now().UnixMilli(), 10)
		items, err := c.client.ZRangeByScore(context.Background(), bucket, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   now,
			Count: 100,
		}).Result()
		if err != nil && err != redis.Nil {
			logRedisError("delay scan", name, err)
			sleepOrDone(c.done, time.Second)
			continue
		}
		if len(items) == 0 {
			sleepOrDone(c.done, time.Second)
			continue
		}
		for _, item := range items {
			removed, err := c.client.ZRem(context.Background(), bucket, item).Result()
			if err != nil || removed == 0 {
				if err != nil {
					logRedisError("delay remove", name, err)
				}
				continue
			}
			msg, err := redisDelayedMessageValue(item)
			if err != nil {
				logRedisError("delay decode", name, err)
				continue
			}
			if err := c.publish(name, msg); err != nil {
				logRedisError("delay publish", name, err)
				_ = c.client.ZAdd(context.Background(), bucket, redis.Z{Score: float64(time.Now().UnixMilli() + 1000), Member: item}).Err()
			}
		}
	}
}

func (c *redisConnection) cleanConsumers() {
	ticker := time.NewTicker(c.idleClean)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for name := range c.queues {
				c.cleanupIdleConsumers(name)
			}
		case <-c.done:
			return
		}
	}
}

func (c *redisConnection) cleanupIdleConsumers(name string) {
	if c.idleClean <= 0 {
		return
	}
	stream := redisStreamName(name)
	consumers, err := c.client.XInfoConsumers(context.Background(), stream, c.group).Result()
	if err != nil {
		return
	}
	for _, consumer := range consumers {
		if consumer.Pending == 0 && consumer.Idle >= c.idleClean {
			if err := c.client.XGroupDelConsumer(context.Background(), stream, c.group, consumer.Name).Err(); err != nil {
				logRedisError("cleanup consumer", name, err)
			}
		}
	}
}

func (c *redisConnection) migrateLegacyLists(name string) error {
	for _, key := range []string{name, legacyProcessingName(name)} {
		if err := c.migrateLegacyList(name, key); err != nil {
			return err
		}
	}
	return nil
}

func (c *redisConnection) migrateLegacyList(name, key string) error {
	ctx := context.Background()
	kind, err := c.client.Type(ctx, key).Result()
	if err != nil || kind != "list" {
		return err
	}
	for {
		raw, err := c.client.RPop(ctx, key).Result()
		if err == redis.Nil {
			break
		}
		if err != nil {
			return err
		}
		msg := redisMessage{}
		if err := infra.Unmarshal(infra.JSON, []byte(raw), &msg); err != nil {
			logRedisError("migrate decode", name, err)
			continue
		}
		if err := c.publish(name, &msg); err != nil {
			return err
		}
	}
	return c.client.Del(ctx, key).Err()
}

func (c *redisConnection) Publish(name string, data []byte) error {
	return c.publish(name, &redisMessage{
		Attempt: 1,
		After:   0,
		Data:    data,
	})
}

func (c *redisConnection) DeferredPublish(name string, data []byte, delay time.Duration) error {
	return c.delay(name, &redisMessage{
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

func redisMessageValues(msg *redisMessage) map[string]any {
	return map[string]any{
		"attempt": msg.Attempt,
		"after":   msg.After,
		"data":    msg.Data,
	}
}

func redisStreamName(name string) string {
	return name + ":stream"
}

func redisDelayName(name string) string {
	return name + ":delayed"
}

func legacyProcessingName(name string) string {
	return name + ":processing"
}

func redisStreamMessage(raw redis.XMessage) (*redisMessage, error) {
	attempt, err := redisIntValue(raw.Values["attempt"])
	if err != nil {
		return nil, err
	}
	after, err := redisInt64Value(raw.Values["after"])
	if err != nil {
		return nil, err
	}
	data, err := redisBytesValue(raw.Values["data"])
	if err != nil {
		return nil, err
	}
	return &redisMessage{Attempt: attempt, After: after, Data: data}, nil
}

func redisDelayedMember(msg *redisMessage) (string, error) {
	data, err := json.Marshal(redisDelayedMessage{
		ID:      fmt.Sprintf("%d", time.Now().UnixNano()),
		Attempt: msg.Attempt,
		After:   msg.After,
		Data:    msg.Data,
	})
	return string(data), err
}

func redisDelayedMessageValue(value string) (*redisMessage, error) {
	msg := redisDelayedMessage{}
	if err := json.Unmarshal([]byte(value), &msg); err != nil {
		return nil, err
	}
	return &redisMessage{Attempt: msg.Attempt, After: msg.After, Data: msg.Data}, nil
}

func redisIntValue(value any) (int, error) {
	v, err := redisInt64Value(value)
	return int(v), err
}

func redisInt64Value(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("invalid integer value %T", value)
	}
}

func redisBytesValue(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("invalid bytes value %T", value)
	}
}

func stringSetting(setting map[string]any, key, fallback string) string {
	if v, ok := setting[key].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

func durationSetting(setting map[string]any, key string, fallback time.Duration) time.Duration {
	value, ok := setting[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case time.Duration:
		return v
	case int:
		return time.Duration(v) * time.Second
	case int64:
		return time.Duration(v) * time.Second
	case float64:
		return time.Duration(v * float64(time.Second))
	case string:
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}

func int64Setting(setting map[string]any, key string, fallback int64) int64 {
	value, ok := setting[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func redisConsumerName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "host"
	}
	return fmt.Sprintf("%s-%d-%d", streamToken(host), os.Getpid(), time.Now().UnixNano())
}

func streamToken(value string) string {
	replacer := strings.NewReplacer(":", "_", "/", "_", "\\", "_", " ", "_", ".", "_")
	return replacer.Replace(value)
}

func sleepOrDone(done <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-done:
		return false
	}
}

func logRedisError(action, name string, err error) {
	if err != nil {
		fmt.Printf("infrago queue redis %s failed on %s: %v\n", action, name, err)
	}
}

var redisStreamAckScript = redis.NewScript(`
if tonumber(ARGV[6]) and tonumber(ARGV[6]) > 0 then
  redis.call("XADD", KEYS[2], "MAXLEN", "~", ARGV[6], "*", "attempt", ARGV[3], "after", ARGV[4], "data", ARGV[5])
else
  redis.call("XADD", KEYS[2], "*", "attempt", ARGV[3], "after", ARGV[4], "data", ARGV[5])
end
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("XDEL", KEYS[1], ARGV[2])
return 1
`)

var redisDelayAckScript = redis.NewScript(`
redis.call("ZADD", KEYS[2], ARGV[3], ARGV[4])
redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
redis.call("XDEL", KEYS[1], ARGV[2])
return 1
`)

var _ queue.Connection = (*redisConnection)(nil)
