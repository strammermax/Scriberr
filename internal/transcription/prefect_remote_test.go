package transcription

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"scriberr/internal/models"
)

func TestPrefectEligibilityPreservesLocalFeatures(t *testing.T) {
	t.Setenv("PREFECT_AUDIO_BRIDGE_URL", "http://bridge")
	p := models.WhisperXParams{ModelFamily: "whisper", Model: "small", Task: "transcribe", NoAlign: true}
	if !prefectEligible(p) {
		t.Fatal("remote profile should dispatch")
	}
	for _, mutate := range []func(*models.WhisperXParams){
		func(p *models.WhisperXParams) { p.Diarize = true },
		func(p *models.WhisperXParams) { p.NoAlign = false },
		func(p *models.WhisperXParams) { p.Task = "translate" },
		func(p *models.WhisperXParams) { p.Model = "large-v3" },
	} {
		other := p
		mutate(&other)
		if prefectEligible(other) {
			t.Fatal("unsupported feature must stay local")
		}
	}
}

func TestPrefectHandoffImportsSegments(t *testing.T) {
	folder := t.TempDir()
	audio := filepath.Join(folder, "audio.mp3")
	token := filepath.Join(folder, "token")
	if err := os.WriteFile(audio, []byte("audio bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing authentication")
		}
		if r.Method == "POST" {
			if r.ContentLength != 11 {
				t.Errorf("audio size=%d", r.ContentLength)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"job_id": "0123456789abcdef0123456789abcdef", "run_id": "run"})
		} else {
			_, _ = w.Write([]byte(`{"status":"completed","client":"pc-47","result":{"text":"Hallo","segments":[{"start":1.2,"end":2.5,"text":"Hallo"}],"language":"nl","engine":"whisper.cpp-cuda","elapsed_seconds":1}}`))
		}
	}))
	defer srv.Close()
	t.Setenv("PREFECT_AUDIO_BRIDGE_URL", srv.URL)
	t.Setenv("PREFECT_AUDIO_TOKEN_FILE", token)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := prefectTranscript(ctx, audio, "nl")
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "Hallo" || result.Segments[0].Start != 1.2 || result.Metadata["worker"] != "pc-47" {
		t.Fatalf("bad imported transcript: %+v", result)
	}
}
