# queue-redis

`queue-redis` 是 `github.com/infrago/queue` 的**redis 驱动**。

## 包定位

- 类型：驱动
- 作用：把 `queue` 模块的统一接口落到 `redis` 后端实现

## 快速接入

```go
import (
    _ "github.com/infrago/queue"
    _ "github.com/infrago/queue-redis"
)
```

```toml
[queue]
driver = "redis"
```

## `setting` 专用配置项

配置位置：`[queue].setting`

- `port`
- `server`
- `host`
- `addr`
- `username`
- `password`
- `database`
- `group`
- `consumer`
- `claim`
- `consumer_idle`
- `maxlen`

## 说明

- 当前实现使用 Redis Streams consumer group，stream key 为 `{queue}:stream`
- 延迟消息使用 `{queue}:delayed` sorted set，到期后搬入 stream
- 旧版 list 数据会在启动时从 `{queue}` 和 `{queue}:processing` 迁移到 stream
- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
