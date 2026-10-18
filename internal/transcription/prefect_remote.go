package transcription

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// bridgeAuth returns the audio bridge base URL and its bearer header value.
func bridgeAuth() (string, string, error) {
	base := strings.TrimRight(os.Getenv("PREFECT_AUDIO_BRIDGE_URL"), "/")
	token, err := os.ReadFile(os.Getenv("PREFECT_AUDIO_TOKEN_FILE"))
	if base == "" || err != nil || len(strings.TrimSpace(string(token))) == 0 {
		return "", "", fmt.Errorf("Prefect audio bridge not configured")
	}
	return base, "Bearer " + strings.TrimSpace(string(token)), nil
}

// memoHeaders passes the publication settings to the bridge (URL-encoded UTF-8).
func memoHeaders(req *http.Request, job *models.TranscriptionJob) {
	m := job.Memo
	if m.Project == "" {
		return
	}
	set := func(name, value string) {
		if value != "" {
			req.Header.Set(name, url.QueryEscape(value))
		}
	}
	set("X-Project", m.Project)
	if job.Title != nil {
		set("X-Title", *job.Title)
	}
	set("X-Filename", filepath.Base(job.AudioPath))
	set("X-Meeting-Type", m.MeetingType)
	set("X-Outputs", m.Outputs)
	set("X-Attendees", strings.ReplaceAll(m.Attendees, "\n", ", "))
	if m.Speakers > 0 {
		set("X-Speakers", fmt.Sprint(m.Speakers))
	}
	recorded := job.CreatedAt
	if m.RecordedAt != nil {
		recorded = *m.RecordedAt
	}
	if !recorded.IsZero() {
		set("X-Recorded-At", recorded.Format(time.RFC3339))
	}
}

func prefectTranscript(ctx context.Context, job *models.TranscriptionJob, language string) (*interfaces.TranscriptResult, error) {
	base, auth, err := bridgeAuth()
	if err != nil {
		return nil, err
	}
	audio := job.AudioPath
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
	req.Header.Set("Authorization", auth)
	memoHeaders(req, job)
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
	if job.Memo.LiveText != "" {
		// Before the publication runs: the bridge keeps it next to the audio as livetekst.md.
		liveReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/livetext/"+accepted.JobID,
			strings.NewReader(job.Memo.LiveText))
		if err == nil {
			liveReq.Header.Set("Authorization", auth)
			liveReq.Header.Set("Content-Type", "text/markdown; charset=utf-8")
			var stored map[string]string
			if err := prefectJSON(client, liveReq, &stored); err != nil {
				logger.Warn("Live text not passed to the bridge", "error", err)
			}
		}
	}
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
				Metadata: map[string]string{"prefect_run_id": accepted.RunID, "worker": reply.Client,
					"bridge_job_id": accepted.JobID}}, nil
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
	result, err := prefectTranscript(ctx, job, language)
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

// PrefectProjects lists the project folders known to the audio bridge (Nextcloud Projects/).
func PrefectProjects(ctx context.Context) ([]string, error) {
	return projectCall(ctx, http.MethodGet, "")
}

// PrefectCreateProject creates a project folder and returns the updated list.
func PrefectCreateProject(ctx context.Context, name string) ([]string, error) {
	return projectCall(ctx, http.MethodPost, name)
}

func projectCall(ctx context.Context, method, name string) ([]string, error) {
	base, auth, err := bridgeAuth()
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if name != "" {
		data, _ := json.Marshal(map[string]string{"name": name})
		body = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, base+"/projects", body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	var reply struct {
		Projects []string `json:"projects"`
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if err := prefectJSON(client, req, &reply); err != nil {
		return nil, err
	}
	return reply.Projects, nil
}

// MemoPublication is how far the Nextcloud publication of a memo is.
type MemoPublication struct {
	Status string `json:"status"` // busy | published | failed (empty before the first step)
	Stage  string `json:"stage,omitempty"` // speakers | summary | quotes | report | actions | nextcloud
	Folder string `json:"folder,omitempty"`
	FileID string `json:"fileid,omitempty"` // Nextcloud file ID of the folder: https://<cloud>/f/<id>
	Error  string `json:"error,omitempty"`
}

// PrefectPublication asks the bridge whether a handed-off recording is published.
func PrefectPublication(ctx context.Context, bridgeJobID string) (*MemoPublication, error) {
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(bridgeJobID) {
		return nil, fmt.Errorf("Invalid Prefect handoff ID")
	}
	base, auth, err := bridgeAuth()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/jobs/"+bridgeJobID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", auth)
	var reply struct {
		Publication MemoPublication `json:"publication"`
	}
	if err := prefectJSON(&http.Client{Timeout: 30 * time.Second}, req, &reply); err != nil {
		return nil, err
	}
	return &reply.Publication, nil
}
