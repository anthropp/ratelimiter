package config

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const valid = `
lease:
  size: 10
  durationMs: 2000
tenants:
  tenant-a: 100
  tenant-b: 50
`

func TestLoadValid(t *testing.T) {
	c, err := Load(write(t, valid))
	if err != nil {
		t.Fatal(err)
	}
	if c.Lease.Size != 10 || c.LeaseDuration().Milliseconds() != 2000 {
		t.Fatalf("lease = %+v", c.Lease)
	}
	if c.Tenants["tenant-a"] != 100 || c.Tenants["tenant-b"] != 50 {
		t.Fatalf("tenants = %v", c.Tenants)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"zero lease size": "lease: {size: 0, durationMs: 2000}\ntenants: {a: 1}\n",
		"tiny duration":   "lease: {size: 10, durationMs: 50}\ntenants: {a: 1}\n",
		"no tenants":      "lease: {size: 10, durationMs: 2000}\ntenants: {}\n",
		"negative rate":   "lease: {size: 10, durationMs: 2000}\ntenants: {a: -5}\n",
		"unknown field":   "lease: {size: 10, durationMs: 2000}\ntenants: {a: 1}\nsurprise: true\n",
		"not yaml":        "{{{{",
	}
	for name, content := range cases {
		if _, err := Load(write(t, content)); err == nil {
			t.Errorf("%s: Load succeeded, want error", name)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("Load of missing file succeeded, want error")
	}
}
