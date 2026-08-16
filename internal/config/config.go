// Package config parses the coordinator's ConfigMap-mounted YAML config.
package config

import (
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// BurstSeconds sets each tenant's bucket capacity to rate × BurstSeconds.
// Deliberately not configurable (design decision D6).
const BurstSeconds = 1.0

type Lease struct {
	Size       int   `json:"size"`
	DurationMs int64 `json:"durationMs"`
}

type Config struct {
	Lease   Lease              `json:"lease"`
	Tenants map[string]float64 `json:"tenants"` // tenant -> limit in req/s
}

func (c *Config) LeaseDuration() time.Duration {
	return time.Duration(c.Lease.DurationMs) * time.Millisecond
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.UnmarshalStrict(raw, &c); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if c.Lease.Size < 1 {
		return nil, fmt.Errorf("lease.size must be >= 1, got %d", c.Lease.Size)
	}
	if c.Lease.DurationMs < 100 {
		return nil, fmt.Errorf("lease.durationMs must be >= 100, got %d", c.Lease.DurationMs)
	}
	if len(c.Tenants) == 0 {
		return nil, fmt.Errorf("no tenants configured")
	}
	for name, rate := range c.Tenants {
		if rate <= 0 {
			return nil, fmt.Errorf("tenant %q: rate must be > 0, got %v", name, rate)
		}
	}
	return &c, nil
}
