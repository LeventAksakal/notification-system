package fakeprovider_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/LeventAksakal/notification-system/internal/fakeprovider"
)

func TestHistoryIsCappedWhileCountersStayExact(t *testing.T) {
	s, url, _ := start(t, fakeprovider.WithHistoryLimit(3))
	for i := range 5 {
		send(t, nil, http.MethodPost, url, sms, "Idempotency-Key", fmt.Sprint("k", i))
	}
	reqs := s.Requests(fakeprovider.Filter{})
	if len(reqs) != 3 || reqs[0].IdempotencyKey != "k2" || reqs[2].IdempotencyKey != "k4" {
		t.Errorf("history %+v, want k2..k4", reqs)
	}
	if st := s.Stats(); st.Channels["sms"].Deliveries != 5 || st.DistinctKeys != 5 {
		t.Errorf("counters %+v", st)
	}
}
