package transcription

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"scriberr/internal/models"
	"scriberr/internal/transcription/interfaces"
	"scriberr/pkg/logger"
)

// Remote workers currently expose small/transcribe with segment timestamps.
// Keep alignment, diarization, translation and other model families local.
func prefectEligible(p models.WhisperXParams) bool {
	return os.Getenv("PREFECT_AUDIO_BRIDGE_URL") != "" &&
		p.ModelFamily == FamilyWhisper && p.Model == "small" &&
		p.Task == "transcribe" && !p.Diarize && p.NoAlign && !p.ReturnCharAlignments &&
		(p.InitialPrompt == nil || *p.InitialPrompt == "")
}

func prefectTranscript(ctx context.Context, audio, language string) (*interfaces.TranscriptResult, error) {
	base := strings.TrimRight(os.Getenv("PREFECT_AUDIO_BRIDGE_URL"), "/")
	tokenPath := os.Getenv("PREFECT_AUDIO_TOKEN_FILE")
	token, err := os.ReadFile(tokenPath)
	if err != nil || len(strings.TrimSpace(string(token))) == 0 {
		return nil, fmt.Errorf("Prefect audio token file unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()
	client := &http.Client{Timeout: 150 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return fmt.Errorf("Prefect audio redirects are forbidden")
	}}
	file, err := os.Open(audio)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/jobs", file)
	if err != nil {
		return nil, err
	}
	req.ContentLength = info.Size()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Audio-Language", language)
	auth := "Bearer " + strings.TrimSpace(string(token))
	req.Header.Set("Authorization", auth)
	var accepted struct {
		JobID string `json:"job_id"`
		RunID string `json:"run_id"`
	}
	if err := prefectJSON(client, req, &accepted); err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(accepted.JobID) {
		return nil, fmt.Errorf("Invalid Prefect handoff ID")
	}
	logger.Info("Audio submitted to Prefect", "run_id", accepted.RunID)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, base+"/jobs/"+accepted.JobID, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", auth)
		var reply struct {
			Status string `json:"status"`
			Error  string `json:"error"`
			Client string `json:"client"`
			Result struct {
				Text     string                         `json:"text"`
				Language string                         `json:"language"`
				Segments []interfaces.TranscriptSegment `json:"segments"`
				Engine   string                         `json:"engine"`
				Elapsed  float64                        `json:"elapsed_seconds"`
			} `json:"result"`
		}
		if err := prefectJSON(client, req, &reply); err != nil {
			return nil, err
		}
		switch reply.Status {
		case "completed":
			if reply.Result.Segments == nil {
				return nil, fmt.Errorf("Prefect returned no segment list")
			}
			return &interfaces.TranscriptResult{Text: reply.Result.Text, Language: reply.Result.Language,
				Segments: reply.Result.Segments, ModelUsed: reply.Result.Engine + ":small",
				ProcessingTime: time.Duration(reply.Result.Elapsed * float64(time.Second)),
				Metadata:       map[string]string{"prefect_run_id": accepted.RunID, "worker": reply.Client}}, nil
		case "failed":
			return nil, fmt.Errorf("%s", reply.Error)
		case "pending":
		default:
			return nil, fmt.Errorf("Invalid Prefect handoff status")
		}
	}
}

func prefectJSON(client *http.Client, req *http.Request, out interface{}) error {
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Prefect audio bridge unavailable: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("Prefect audio bridge returned HTTP %d", res.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(res.Body, 16*1024*1024)).Decode(out)
}

func (u *UnifiedTranscriptionService) processPrefectJob(ctx context.Context, job *models.TranscriptionJob) error {
	language := "auto"
	if job.Parameters.Language != nil && *job.Parameters.Language != "" {
		language = *job.Parameters.Language
	}
	result, err := prefectTranscript(ctx, job.AudioPath, language)
	if err != nil {
		return err
	}
	// Keep portable exports beside Scriberr's normal persisted transcript.
	folder := filepath.Join(u.outputDirectory, job.ID)
	if err := os.MkdirAll(folder, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(folder, "transcript.json"), data, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(folder, "transcript.txt"), []byte(result.Text), 0600); err != nil {
		return err
	}
	if err := u.saveTranscriptionResults(job.ID, result); err != nil {
		return err
	}
	transcript := string(data)
	job.Transcript = &transcript
	return nil
}
