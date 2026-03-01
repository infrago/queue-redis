# queue-redis

`queue-redis` 是 `queue` 模块的 `redis` 驱动。

## 安装

```bash
go get github.com/infrago/queue@latest
go get github.com/infrago/queue-redis@latest
```

## 接入

```go
import (
    _ "github.com/infrago/queue"
    _ "github.com/infrago/queue-redis"
    "github.com/infrago/infra"
)

func main() {
    infra.Run()
}
```

## 配置示例

```toml
[queue]
driver = "redis"
```

## 公开 API（摘自源码）

- `func (d *redisDriver) Connect(inst *queue.Instance) (queue.Connection, error)`
- `func (c *redisConnection) Open() error`
- `func (c *redisConnection) Close() error`
- `func (c *redisConnection) Register(name string) error`
- `func (c *redisConnection) Start() error`
- `func (c *redisConnection) Stop() error`
- `func (c *redisConnection) Publish(name string, data []byte) error`
- `func (c *redisConnection) DeferredPublish(name string, data []byte, delay time.Duration) error`

## 排错

- driver 未生效：确认模块段 `driver` 值与驱动名一致
- 连接失败：检查 endpoint/host/port/鉴权配置
