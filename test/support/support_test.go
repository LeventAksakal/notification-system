package support_test

import (
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/LeventAksakal/notification-system/test/support"
)

func TestProviderHelperServesTheDouble(t *testing.T) {
	url, s := support.Provider(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url+"/send",
		strings.NewReader(`{"to":"+905551234567","channel":"push","content":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusAccepted || s.Stats().Channels["push"].Deliveries != 1 {
		t.Errorf("status %d, stats %+v", res.StatusCode, s.Stats())
	}
}

func TestImagesMatchCompose(t *testing.T) {
	compose, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	for _, img := range []string{support.MySQLImage, support.ValkeyImage} {
		if !strings.Contains(string(compose), "image: "+img) {
			t.Errorf("docker-compose.yml does not use %s", img)
		}
	}
}
