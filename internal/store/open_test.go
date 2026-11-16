package store

import (
	"testing"
	"time"
)

func TestDriverTimeoutsDefaultWhenTheDSNSetsNone(t *testing.T) {
	for _, tc := range []struct {
		name, dsn         string
		dial, read, write time.Duration
	}{
		{"unset: dial 5s, read and write 30s", "u:p@tcp(db:3306)/app", 5 * time.Second, 30 * time.Second, 30 * time.Second},
		{"set: the DSN's values are kept", "u:p@tcp(db:3306)/app?timeout=2s&readTimeout=7s&writeTimeout=9s",
			2 * time.Second, 7 * time.Second, 9 * time.Second},
		{"partly set: only the missing ones default", "u:p@tcp(db:3306)/app?readTimeout=1m",
			5 * time.Second, time.Minute, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := driverConfig(tc.dsn)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Timeout != tc.dial || cfg.ReadTimeout != tc.read || cfg.WriteTimeout != tc.write {
				t.Fatalf("dial %s read %s write %s, want %s %s %s", cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout, tc.dial, tc.read, tc.write)
			}
			if !cfg.ParseTime || !cfg.InterpolateParams {
				t.Fatalf("forced options lost: %+v", cfg)
			}
		})
	}
}
