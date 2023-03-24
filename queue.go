package queue_redis

import (
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/infrago/infra"
	"github.com/infrago/log"
	"github.com/infrago/queue"
	"github.com/infrago/util"
)

var (
	errInvalidConnection = errors.New("Invalid queue connection.")
	errAlreadyRunning    = errors.New("Redis queue is already running.")
	errNotRunning        = errors.New("Redis queue is not running.")
)

type (
	redisDriver  struct{}
	redisConnect struct {
		mutex  sync.RWMutex
		client *redis.Pool

		running bool
		actives int64

		instance *queue.Instance
		setting  redisSetting

		queues []queue.Info
		// stopers map[string]string
		subs []redis.Conn
	}
	//配置文件
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}

	redisMsg struct {
		Attempt int    `json:"t"`
		After   int64  `json:"a"`
		Data    []byte `json:"d"`
	}
)

// 连接
func (driver *redisDriver) Connect(inst *queue.Instance) (queue.Connect, error) {
	//获取配置信息
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := inst.Config.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := inst.Config.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := inst.Config.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := inst.Config.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := inst.Config.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := inst.Config.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		instance: inst, setting: setting,
		queues: make([]queue.Info, 0),
		// stopers: make(map[string]string, 0),
		subs: make([]redis.Conn, 0),
	}, nil
}

// 打开连接
func (this *redisConnect) Open() error {
	this.client = &redis.Pool{
		MaxIdle: this.setting.Idle, MaxActive: this.setting.Active, IdleTimeout: this.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", this.setting.Server)
			if err != nil {
				log.Warning("queue.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if this.setting.Password != "" {
				if _, err := c.Do("AUTH", this.setting.Password); err != nil {
					c.Close()
					log.Warning("queue.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if this.setting.Database != "" {
				if _, err := c.Do("SELECT", this.setting.Database); err != nil {
					c.Close()
					log.Warning("queue.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := this.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

func (this *redisConnect) Health() (queue.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return queue.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *redisConnect) Close() error {
	if this.client != nil {
		if err := this.client.Close(); err != nil {
			return err
		}
		this.client = nil
	}
	return nil
}

func (this *redisConnect) Register(info queue.Info) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.queues = append(this.queues, info)

	return nil
}

// 开始订阅者
func (this *redisConnect) Start() error {
	if this.running {
		return errAlreadyRunning
	}

	//这个循环，用来从redis读消息
	for _, info := range this.queues {
		queName := info.Name

		conn, err := this.client.Dial()
		if err != nil {
			return err
		}

		this.subs = append(this.subs, conn)

		//走全局的池
		this.instance.Submit(func() {
			for {
				bytes, err := redis.ByteSlices(conn.Do("BRPOP", queName, 1))
				if err != nil {
					if err == redis.ErrNil {
						continue //空数据继续
					} else {
						break //退出循环
					}
				}

				if bytes != nil && len(bytes) >= 2 {
					gotName := string(bytes[0])
					gotData := bytes[1]

					msg := &redisMsg{}
					err := infra.Unmarshal(this.instance.Config.Codec, gotData, msg)
					if err != nil {
						continue //失败就丢弃消息
					}

					now := time.Now()

					//没到处理时间，延迟1秒写回队列
					if msg.After > 0 && msg.After > now.Unix() {
						time.Sleep(time.Second)
						this.publish(gotName, msg)
						continue //跳过
					}

					req := queue.Request{
						gotName, msg.Data, msg.Attempt, time.Now(),
					}

					res := this.instance.Serve(req)
					if res.Retry {
						msg.Attempt++
						msg.After = now.Add(res.Delay).Unix()
						this.publish(gotName, msg)
					}
				}
			}
		})

	}

	this.running = true
	return nil
}

// 停止订阅
func (this *redisConnect) Stop() error {
	if false == this.running {
		return errNotRunning
	}

	//关闭订阅
	for _, sub := range this.subs {
		sub.Close()
	}

	// for _, stoper := range this.stopers {
	// 	if stoper != "" {
	// 		this.Publish(stoper, nil)
	// 	}
	// }

	this.running = false
	return nil
}

func (this *redisConnect) publish(name string, msg *redisMsg) error {
	if this.client == nil {
		return errInvalidConnection
	}
	conn := this.client.Get()
	defer conn.Close()

	bytes, err := infra.Marshal(this.instance.Config.Codec, msg)
	if err != nil {
		log.Warning("queue.redis.enqueue", err)
		return err
	}

	//写入
	_, err = conn.Do("LPUSH", name, bytes)

	if err != nil {
		log.Warning("queue.redis.enqueue", err)
		return err
	}

	return nil
}

func (this *redisConnect) Publish(name string, data []byte) error {
	if this.client == nil {
		return errInvalidConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	msg := &redisMsg{1, 0, data}

	return this.publish(name, msg)
}

// DeferredPublish redis 暂不支持延迟队列
func (this *redisConnect) DeferredPublish(name string, data []byte, delay time.Duration) error {
	if this.client == nil {
		return errInvalidConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	after := time.Now().Add(delay).Unix()
	msg := &redisMsg{1, after, data}

	return this.publish(name, msg)
}
