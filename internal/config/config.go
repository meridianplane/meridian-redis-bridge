package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// RouteRule is one key-prefix routing entry. First match wins.
type RouteRule struct {
	Prefix string `json:"prefix"`
	Target string `json:"target"`
}

// Config is the full node configuration loaded from JSON.
type Config struct {
	Cluster string `json:"cluster,omitempty"`
	Name    string `json:"name,omitempty"`

	Upstream []string `json:"upstream,omitempty"`
	Relay    bool     `json:"relay,omitempty"`

	// Routes maps key prefixes to primary targets for cross-shard write
	// forwarding. Longest matching prefix wins (order-independent).
	Routes []RouteRule `json:"routes,omitempty"`

	// Primaries maps primary names to their gRPC endpoints.
	Primaries map[string][]string `json:"primaries,omitempty"`

	Listen        string `json:"listen"`
	GRPCListen    string `json:"grpc_listen"`
	MetricsListen string `json:"metrics_listen,omitempty"`
	ForwardWrites *bool  `json:"forward_writes,omitempty"`
	WALFlush      string `json:"wal_flush,omitempty"`
	WALFlushInterval int `json:"wal_flush_interval,omitempty"`
	Auth          AuthConfig   `json:"auth,omitempty"`
	Backend       BackendConfig `json:"backend"`
	DataDir       string `json:"data_dir"`
}

type BackendConfig struct {
	Addr     string   `json:"addr,omitempty"`
	Addrs    []string `json:"addrs,omitempty"`
	Username string   `json:"username,omitempty"`
	Password string   `json:"password,omitempty"`
	DB       int      `json:"db,omitempty"`
	PoolSize int      `json:"pool_size,omitempty"`
}

type AuthConfig struct {
	PasswdFile string `json:"passwd_file,omitempty"`
}

func (c *Config) WALDir() string  { return c.DataDir + "/wal" }
func (c *Config) StateDir() string { return c.DataDir + "/state" }

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	raw := os.Expand(string(data), func(k string) string { return os.Getenv(k) })
	var c Config
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("config %s: %w", path, err)
	}
	return &c, nil
}

func (c *Config) validate() error {
	for _, addr := range c.Upstream {
		if addr == "" {
			return fmt.Errorf("upstream contains empty address")
		}
	}
	if c.GRPCListen == "" {
		return fmt.Errorf("grpc_listen is required")
	}
	if c.IsLB() {
		if len(c.Upstream) == 0 {
			return fmt.Errorf("lb mode requires upstream addresses")
		}
		return nil
	}
	if c.Listen == "" {
		return fmt.Errorf("listen is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.IsPrimary() && c.Backend.Addr == "" && len(c.Backend.Addrs) == 0 {
		return fmt.Errorf("primary node must have a backend configured")
	}
	return nil
}

func (c *Config) IsPrimary() bool        { return len(c.Upstream) == 0 }
func (c *Config) UpstreamAddrs() []string { return c.Upstream }

func (c *Config) IsLB() bool {
	return !c.IsPrimary() && !c.Relay && c.Backend.Addr == "" && len(c.Backend.Addrs) == 0
}

func (c *Config) AuthEnabled() bool { return c.Auth.PasswdFile != "" }

func (c *Config) ForwardWritesEnabled() bool {
	return c.ForwardWrites == nil || *c.ForwardWrites
}
