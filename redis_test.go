package queue_redis

import "testing"

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

func TestProcessingName(t *testing.T) {
	if got := processingName("jobs"); got != "jobs:processing" {
		t.Fatalf("processing=%q", got)
	}
}
