package transcription

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

// LiveResult is the text of one short live chunk.
type LiveResult struct {
	Text     string `json:"text"`
	Language string `json:"language,omitempty"`
	Worker   string `json:"worker,omitempty"`
}

// LiveProvider turns a short audio chunk (Opus, WAV, ...) into text while a recording is still running.
// "bridge" uses the own GPU workers; an online service can be added as another provider
// (for example on top of the OpenAI adapter) and chosen with MEMORECORDER_LIVE_PROVIDER.
type LiveProvider interface {
	Transcribe(ctx context.Context, audio []byte, language string) (*LiveResult, error)
}

// NewLiveProvider returns the provider configured for live text.
func NewLiveProvider() (LiveProvider, error) {
	switch provider := os.Getenv("MEMORECORDER_LIVE_PROVIDER"); provider {
	case "", "bridge":
		return bridgeLive{}, nil
	default:
		return nil, fmt.Errorf("unknown live provider %q", provider)
	}
}

// bridgeLive sends the chunk to the audio bridge, which runs it on the NVIDIA or AMD worker.
type bridgeLive struct{}

func (bridgeLive) Transcribe(ctx context.Context, audio []byte, language string) (*LiveResult, error) {
	base, auth, err := bridgeAuth()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/live", bytes.NewReader(audio))
	if err != nil {
		return nil, err
	}
	req.ContentLength = int64(len(audio))
	req.Header.Set("Content-Type", "audio/wav")
	req.Header.Set("X-Audio-Language", language)
	req.Header.Set("Authorization", auth)
	var reply struct {
		Text     string `json:"text"`
		Language string `json:"language"`
		Client   string `json:"client"`
	}
	// Bridge waits up to ~25 s for a free GPU plus the transcription itself.
	if err := prefectJSON(&http.Client{Timeout: 180 * time.Second}, req, &reply); err != nil {
		return nil, err
	}
	return &LiveResult{Text: reply.Text, Language: reply.Language, Worker: reply.Client}, nil
}
