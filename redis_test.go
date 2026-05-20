package queue_redis

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisAddr(t *testing.T) {
	cases := []struct {
		name    string
		setting map[string]any
		addr    string
	}{
		{name: "default", setting: map[string]any{}, addr: "127.0.0.1:6379"},
		{name: "addr", setting: map[string]any{"addr": "redis:6380"}, addr: "redis:6380"},
		{name: "host port", setting: map[string]any{"host": "redis", "port": "6380"}, addr: "redis:6380"},
		{name: "server with port", setting: map[string]any{"server": "redis:6380"}, addr: "redis:6380"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := redisAddr(tc.setting); got != tc.addr {
				t.Fatalf("addr=%q, want %q", got, tc.addr)
			}
		})
	}
}

func TestRedisStreamMessageRoundTrip(t *testing.T) {
	msg := &redisMessage{Attempt: 3, After: time.Unix(0, 123).UnixNano(), Data: []byte("payload")}
	raw := redis.XMessage{
		ID:     "1-0",
		Values: redisMessageValues(msg),
	}

	got, err := redisStreamMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempt != msg.Attempt || got.After != msg.After || string(got.Data) != string(msg.Data) {
		t.Fatalf("message=%+v, want %+v", got, msg)
	}
}

func TestRedisSettings(t *testing.T) {
	setting := map[string]any{
		"group": "workers",
		"claim": "30s",
	}
	if got := stringSetting(setting, "group", "default"); got != "workers" {
		t.Fatalf("group=%q", got)
	}
	if got := durationSetting(setting, "claim", time.Minute); got != 30*time.Second {
		t.Fatalf("claim=%v", got)
	}
	if got := streamToken("foo.bar/baz:qux"); got != "foo_bar_baz_qux" {
		t.Fatalf("token=%q", got)
	}
	if got := int64Setting(map[string]any{"maxlen": "1000"}, "maxlen", 0); got != 1000 {
		t.Fatalf("maxlen=%d", got)
	}
}

func TestRedisDelayedMessageRoundTrip(t *testing.T) {
	msg := &redisMessage{Attempt: 2, After: time.Now().Add(time.Minute).UnixNano(), Data: []byte("delayed")}
	member, err := redisDelayedMember(msg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := redisDelayedMessageValue(member)
	if err != nil {
		t.Fatal(err)
	}
	if got.Attempt != msg.Attempt || got.After != msg.After || string(got.Data) != string(msg.Data) {
		t.Fatalf("message=%+v, want %+v", got, msg)
	}
	if redisDelayName("jobs") != "jobs:delayed" {
		t.Fatalf("delay key=%q", redisDelayName("jobs"))
	}
}
