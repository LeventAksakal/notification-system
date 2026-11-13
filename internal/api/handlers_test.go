package api

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LeventAksakal/notification-system/internal/store"
	"github.com/LeventAksakal/notification-system/internal/store/sqlc"
)

type fakeQ struct {
	sqlc.Querier
	rows map[string]sqlc.AppMetum
}

func (f fakeQ) GetMeta(_ context.Context, k string) (sqlc.AppMetum, error) {
	if m, ok := f.rows[k]; ok {
		return m, nil
	}
	return sqlc.AppMetum{}, sql.ErrNoRows
}

func serve(t *testing.T, checks map[string]Check, path string) *httptest.ResponseRecorder {
	t.Helper()
	q := fakeQ{rows: map[string]sqlc.AppMetum{"version": {K: "version", V: "1", UpdatedAt: time.Unix(0, 0)}}}
	h, err := NewServer(q, checks).Handler()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return rec
}

func TestClientCanReadAKeyAndGets404ForAMissingOne(t *testing.T) {
	if rec := serve(t, nil, "/v1/meta/version"); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"value":"1"`) {
		t.Fatalf("found key: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, nil, "/v1/meta/nope"); rec.Code != 404 {
		t.Fatalf("missing key: %d %s", rec.Code, rec.Body)
	}
}

func TestSpecRejectsAKeyLongerThanTheLimitBeforeTheHandler(t *testing.T) {
	rec := serve(t, nil, "/v1/meta/"+strings.Repeat("k", store.KeyMaxLen+1))
	if rec.Code != 400 {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestReadinessIs503WhenADependencyIsDown(t *testing.T) {
	down := map[string]Check{"mysql": func(context.Context) error { return errors.New("refused") }}
	if rec := serve(t, down, "/readyz"); rec.Code != 503 || !strings.Contains(rec.Body.String(), `"mysql":"down"`) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

// Rules on every layer: the key limit is one number in the spec, the service
// and the schema.
func TestKeyLimitAgreesAcrossSpecServiceAndSchema(t *testing.T) {
	spec, err := GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	if got := *spec.Paths.Find("/v1/meta/{key}").Get.Parameters[0].Value.Schema.Value.MaxLength; got != store.KeyMaxLen {
		t.Errorf("spec maxLength %d, service %d", got, store.KeyMaxLen)
	}
	ddl, err := os.ReadFile("../../schema/app_meta.sql")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*k VARCHAR\((\d+)\)`).FindSubmatch(ddl)
	if m == nil || string(m[1]) != strconv.Itoa(store.KeyMaxLen) {
		t.Errorf("schema key column does not say VARCHAR(%d)", store.KeyMaxLen)
	}
}
