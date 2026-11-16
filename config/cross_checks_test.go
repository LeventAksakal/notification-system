package config

import (
	"os"
	"strings"
	"testing"
)

func TestEveryDialSaysWhereItsDefaultComesFrom(t *testing.T) {
	for _, d := range Dials {
		if !strings.Contains(d.Doc, "Default") {
			t.Errorf("%s: the doc line needs a clause saying where the default comes from (\"Default: ...\" or "+
				"\"Default target: ...\"): derived, measured, a named default, or a free choice and how to tune it", d.Env)
		}
	}
}

func TestDialsThatCannotWorkTogetherAreRefusedWithTheirRule(t *testing.T) {
	// The lease floor: provider timeout + slot wait + one slot interval +
	// 500ms margin. The slot wait is the alignment's ceiling (lease/8, at
	// least 200ms) plus the larger of the horizon and that ceiling; at 95/s
	// the interval is 10.527ms.
	for name, tc := range map[string]struct {
		kv   map[string]string
		want []string // every string the error must contain; none: accepted
	}{
		"lease shorter than the provider timeout": {
			kv: map[string]string{"APP_LEASE_DURATION": "3s", "APP_PROVIDER_TIMEOUT": "5s"},
			want: []string{"APP_LEASE_DURATION 3s is below APP_PROVIDER_TIMEOUT 5s", "longest slot wait 750ms",
				"one slot interval 10.527ms", "lease margin 500ms", "= 6.260527s", "cuts calls"}},
		"lease just under the floor": {
			kv:   map[string]string{"APP_LEASE_DURATION": "7347ms", "APP_PROVIDER_TIMEOUT": "5s"},
			want: []string{"APP_LEASE_DURATION 7.347s is below"}},
		"lease just over the floor": {
			kv: map[string]string{"APP_LEASE_DURATION": "7348ms", "APP_PROVIDER_TIMEOUT": "5s"}},
		"a horizon above the alignment's ceiling lengthens the wait": {
			kv:   map[string]string{"APP_LEASE_DURATION": "8s", "APP_PROVIDER_TIMEOUT": "5s", "APP_RATE_LIMIT_HORIZON": "2s"},
			want: []string{"longest slot wait 3s", "= 8.510527s"}},
		"a slow pace widens the floor by its slot interval": {
			kv: map[string]string{"APP_LEASE_DURATION": "8s", "APP_PROVIDER_TIMEOUT": "5s", "APP_RATE_LIMIT_PER_SECOND": "1",
				"APP_RATE_LIMIT_HEADROOM_PERCENT": "0"},
			want: []string{"one slot interval 1s", "= 8.5s"}},
		"shutdown grace shorter than the provider timeout": {
			kv:   map[string]string{"APP_SHUTDOWN_TIMEOUT": "4s", "APP_PROVIDER_TIMEOUT": "5s", "APP_REQUEST_TIMEOUT": "2s"},
			want: []string{"APP_SHUTDOWN_TIMEOUT 4s is below APP_PROVIDER_TIMEOUT 5s", "in flight at SIGTERM"}},
		"shutdown grace shorter than the request timeout": {
			kv:   map[string]string{"APP_SHUTDOWN_TIMEOUT": "10s", "APP_REQUEST_TIMEOUT": "15s"},
			want: []string{"APP_SHUTDOWN_TIMEOUT 10s is below APP_REQUEST_TIMEOUT 15s", "unanswered"}},
		"shutdown grace equal to both timeouts": {
			kv: map[string]string{"APP_SHUTDOWN_TIMEOUT": "5s", "APP_REQUEST_TIMEOUT": "5s", "APP_PROVIDER_TIMEOUT": "5s"}},
		"every broken rule is reported at once": {
			kv: map[string]string{"APP_LEASE_DURATION": "1s", "APP_SHUTDOWN_TIMEOUT": "1s", "APP_PROVIDER_TIMEOUT": "2s",
				"APP_RETRY_CAP": "1ms"},
			want: []string{"APP_LEASE_DURATION 1s", "APP_SHUTDOWN_TIMEOUT 1s is below APP_PROVIDER_TIMEOUT",
				"APP_SHUTDOWN_TIMEOUT 1s is below APP_REQUEST_TIMEOUT", "APP_RETRY_CAP"}},
	} {
		t.Run(name, func(t *testing.T) {
			kv := map[string]string{"APP_DATABASE_DSN": "u:p@tcp(db:3306)/app"}
			for k, v := range tc.kv {
				kv[k] = v
			}
			_, err := Load(env(kv))
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want an error containing %q", tc.want)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
		})
	}
}

func TestEnvCheckReportsADialCombinationThatCannotWork(t *testing.T) {
	lines, ok := check([]string{"APP_DATABASE_DSN=u:p@tcp(db:3306)/app", "APP_LEASE_DURATION=3s", "APP_PROVIDER_TIMEOUT=5s"},
		func(string) ([]byte, error) { return nil, os.ErrNotExist })
	if ok {
		t.Fatalf("want a problem, got %q", lines)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "APP_LEASE_DURATION 3s is below APP_PROVIDER_TIMEOUT 5s") {
		t.Errorf("want one line naming the lease rule, got %q", lines)
	}
}
