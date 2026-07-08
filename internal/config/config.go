// Package config loads a node's runtime configuration. Each node is either
// the root (primary) or a child with an upstream — a list of parent gRPC
// addresses. There is no global inventory: the tree is implicit in the
// upstream pointers.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	// Cluster and Name identify this instance in metrics (e.g. cluster="prod-us",
	// name="primary-east"). Exposed as const labels on all Prometheus metrics.
	Cluster string `json:"cluster,omitempty"`
	Name    string `json:"name,omitempty"`

	// Upstream lists the gRPC addresses of parent nodes in the tree. Empty
	// means this node is the root (primary). A follower forwards writes to
	// and subscribes to the WAL of a randomly chosen parent.
	Upstream []string `json:"upstream,omitempty"`

	// Relay enables replication relay: every entry received from upstream
	// is written to the local WAL before being applied.
	Relay bool `json:"relay,omitempty"`

	Listen       string       `json:"listen"`
	GRPCListen    string  `json:"grpc_listen"`
	MetricsListen string  `json:"metrics_listen,omitempty"`
	ForwardWrites *bool   `json:"forward_writes,omitempty"`
	Auth         AuthConfig   `json:"auth,omitempty"`
	Backend BackendConfig `json:"backend"`
	DataDir string        `json:"data_dir"`
}

// WALDir returns the WAL directory under DataDir.
func (c *Config) WALDir() string { return c.DataDir + "/wal" }

// StateDir returns the state directory under DataDir.
func (c *Config) StateDir() string { return c.DataDir + "/state" }

type BackendConfig struct {
	Addr         string   `json:"addr,omitempty"`
	Addrs        []string `json:"addrs,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	DB       int    `json:"db,omitempty"`
	PoolSize     int      `json:"pool_size,omitempty"`
}

type AuthConfig struct {
	PasswdFile string `json:"passwd_file,omitempty"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// ${ENV_VAR} substitution
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
	if c.Listen == "" {
		return fmt.Errorf("listen is required")
	}
	if c.GRPCListen == "" {
		return fmt.Errorf("grpc_listen is required")
	}
	if c.IsLB() {
		// LB mode: upstream required, no data_dir needed.
		if len(c.Upstream) == 0 {
			return fmt.Errorf("lb mode requires upstream addresses")
		}
		return nil
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

// IsLB detects load-balancer mode: upstream configured without relay or
// backend, meaning this node is a pure proxy.
func (c *Config) IsLB() bool {
	return !c.IsPrimary() && !c.Relay && c.Backend.Addr == "" && len(c.Backend.Addrs) == 0
}

// AuthEnabled reports whether client authentication is configured.
func (c *Config) AuthEnabled() bool { return c.Auth.PasswdFile != "" }

func (c *Config) ForwardWritesEnabled() bool {
	return c.ForwardWrites == nil || *c.ForwardWrites
}

