package transcription

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestBridgeLiveProvider(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/live" || r.Header.Get("Authorization") != "Bearer test-token" ||
			r.Header.Get("X-Audio-Language") != "nl" || string(body) != "wav" {
			t.Errorf("unexpected request %s %v %q", r.URL.Path, r.Header, body)
		}
		_, _ = w.Write([]byte(`{"text":"hallo daar","language":"nl","client":"pc-47"}`))
	}))
	defer srv.Close()
	t.Setenv("PREFECT_AUDIO_BRIDGE_URL", srv.URL)
	t.Setenv("PREFECT_AUDIO_TOKEN_FILE", token)
	t.Setenv("MEMORECORDER_LIVE_PROVIDER", "")
	provider, err := NewLiveProvider()
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Transcribe(context.Background(), []byte("wav"), "nl")
	if err != nil || result.Text != "hallo daar" || result.Worker != "pc-47" {
		t.Fatalf("bad live result %+v %v", result, err)
	}
	t.Setenv("MEMORECORDER_LIVE_PROVIDER", "nope")
	if _, err := NewLiveProvider(); err == nil {
		t.Fatal("unknown provider accepted")
	}
}
