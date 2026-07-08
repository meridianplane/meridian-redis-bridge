package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/meridianplane/meridian-redis-bridge/internal/config"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const primaryConfig = `{
  "listen": ":6380",
  "grpc_listen": ":7070",
  "backend": {"addr": "127.0.0.1:6379"},
  "data_dir": "/tmp"
}`

const followerConfig = `{
  "upstream": ["10.0.0.1:7070", "10.0.0.2:7070"],
  "forward_writes": false,
  "listen": ":6380",
  "grpc_listen": ":7071",
  "backend": {"addr": "127.0.0.1:6380"},
  "data_dir": "/tmp"
}`

func TestLoad_Primary(t *testing.T) {
	c, err := config.Load(writeConfig(t, primaryConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.IsPrimary() {
		t.Fatal("empty upstream should be primary")
	}
	if !c.ForwardWritesEnabled() {
		t.Fatal("forward_writes omitted should default to true")
	}
}

func TestLoad_Follower(t *testing.T) {
	c, err := config.Load(writeConfig(t, followerConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.IsPrimary() {
		t.Fatal("non-empty upstream should not be primary")
	}
	if c.ForwardWritesEnabled() {
		t.Fatal("forward_writes:false should disable forwarding")
	}
	if len(c.UpstreamAddrs()) != 2 {
		t.Fatalf("UpstreamAddrs = %v, want 2", c.UpstreamAddrs())
	}
}

func TestLoad_RejectsUnknownField(t *testing.T) {
	body := `{"listen":":1","grpc_listen":":2","backend":{"addr":"a"},"data_dir":"d","typo_field":1}`
	if _, err := config.Load(writeConfig(t, body)); err == nil {
		t.Fatal("unknown field should be rejected")
	}
}

func TestLoad_ValidationErrors(t *testing.T) {
	cases := map[string]string{
		"missing listen":     `{"grpc_listen":":2","backend":{"addr":"a"},"data_dir":"d"}`,
		"missing grpc_listen": `{"listen":":1","backend":{"addr":"a"},"data_dir":"d"}`,
	}
	for name, body := range cases {
		if _, err := config.Load(writeConfig(t, body)); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

func TestLoad_IsLB(t *testing.T) {
	load := func(body string) *config.Config {
		c, err := config.Load(writeConfig(t, body))
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		return c
	}
	if !load(`{"upstream":["h:1"],"grpc_listen":":2"}`).IsLB() {
		t.Fatal("upstream+no backend+no relay should be LB")
	}
	if load(`{"listen":":1","upstream":["h:1"],"grpc_listen":":2","backend":{"addr":"x"},"data_dir":"d"}`).IsLB() {
		t.Fatal("backend.addr should not be LB")
	}
	if load(`{"listen":":1","upstream":["h:1"],"grpc_listen":":2","backend":{"addrs":["a","b"]},"data_dir":"d"}`).IsLB() {
		t.Fatal("backend.addrs (cluster) should not be LB")
	}
	if load(`{"listen":":1","upstream":["h:1"],"grpc_listen":":2","relay":true,"data_dir":"d"}`).IsLB() {
		t.Fatal("relay should not be LB")
	}
	if load(`{"listen":":1","grpc_listen":":2","backend":{"addr":"x"},"data_dir":"d"}`).IsLB() {
		t.Fatal("primary should not be LB")
	}
}

func TestLoad_ProxyAuth(t *testing.T) {
	body := `{"listen":":1","grpc_listen":":2","backend":{"addr":"a"},"data_dir":"d","auth":{"passwd_file":"pw"}}`
	c, err := config.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.AuthEnabled() {
		t.Fatal("auth.passwd_file should report AuthEnabled()==true")
	}
}
