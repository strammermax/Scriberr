package api

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"scriberr/internal/models"
	"scriberr/internal/transcription"

	"github.com/gin-gonic/gin"
)

// Project names become Nextcloud folders: no path separators or control characters.
var projectNamePattern = regexp.MustCompile(`^[^/\\:*?"<>|\x00-\x1f]{1,80}$`)

var memoOutputs = map[string]bool{"plain": true, "verslag": true, "samenvatting": true}

// validMemoSettings checks and normalises the memo form fields.
func validMemoSettings(project, meetingType, speakers, outputs, recordedAt string) (models.MemoSettings, string) {
	m := models.MemoSettings{Project: strings.TrimSpace(project), MeetingType: meetingType}
	if !projectNamePattern.MatchString(m.Project) || strings.Trim(m.Project, ". ") == "" {
		return m, "Invalid project name"
	}
	if m.MeetingType == "" {
		m.MeetingType = "meeting"
	}
	if m.MeetingType != "meeting" && m.MeetingType != "interview" && m.MeetingType != "conversation" {
		return m, "meeting_type must be meeting, interview or conversation"
	}
	if speakers != "" {
		n, err := strconv.Atoi(speakers)
		if err != nil || n < 1 || n > 20 {
			return m, "speakers must be 1..20"
		}
		m.Speakers = n
	}
	var chosen []string
	for _, o := range strings.Split(outputs, ",") {
		if o = strings.TrimSpace(o); o == "" {
			continue
		}
		if !memoOutputs[o] {
			return m, "outputs may only contain plain, verslag, samenvatting"
		}
		chosen = append(chosen, o)
	}
	if len(chosen) == 0 {
		chosen = []string{"plain", "verslag", "samenvatting"}
	}
	m.Outputs = strings.Join(chosen, ",")
	if recordedAt != "" {
		t, err := time.Parse(time.RFC3339, recordedAt)
		if err != nil {
			return m, "recorded_at must be RFC3339, e.g. 2026-10-12T09:30:00+02:00"
		}
		m.RecordedAt = &t
	}
	return m, ""
}

// @Summary Upload a memo recording
// @Description Upload audio with MemoRecorder settings; it is transcribed through Prefect and published to Nextcloud
// @Tags memos
// @Accept multipart/form-data
// @Produce json
// @Param audio formData file true "Audio file"
// @Param project formData string true "Project (Nextcloud folder under Projects/)"
// @Param title formData string false "Title"
// @Param meeting_type formData string false "meeting (default), interview or conversation"
// @Param speakers formData int false "Number of people"
// @Param language formData string false "nl (default), en or auto (detect)"
// @Param outputs formData string false "Comma list of plain, verslag, samenvatting (default all)"
// @Param recorded_at formData string false "Recording start, RFC3339"
// @Success 200 {object} models.TranscriptionJob
// @Router /api/v1/memos [post]
// @Security ApiKeyAuth
// @Security BearerAuth
func (h *Handler) CreateMemo(c *gin.Context) {
	if os.Getenv("PREFECT_AUDIO_BRIDGE_URL") == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Prefect audio bridge not configured"})
		return
	}
	memo, problem := validMemoSettings(c.PostForm("project"), c.PostForm("meeting_type"),
		c.PostForm("speakers"), c.PostForm("outputs"), c.PostForm("recorded_at"))
	if problem != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": problem})
		return
	}
	language := getFormValueWithDefault(c, "language", "nl")
	if language != "nl" && language != "en" && language != "auto" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "language must be nl, en or auto"})
		return
	}
	header, err := c.FormFile(paramAudio)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Audio file is required"})
		return
	}
	filePath, err := h.fileService.SaveUpload(header, h.config.UploadDir)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}
	jobID := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))

	// Parameters that make the job eligible for the Prefect workers (see prefectEligible).
	job := models.TranscriptionJob{
		ID:        jobID,
		AudioPath: filePath,
		Status:    models.StatusPending,
		Parameters: models.WhisperXParams{
			ModelFamily: transcription.FamilyWhisper, Model: "small", Task: "transcribe",
			NoAlign: true, Language: &language, BatchSize: 16, ComputeType: "int8", Device: "cpu",
			VadOnset: 0.500, VadOffset: 0.363,
		},
		Memo: memo,
	}
	title := strings.TrimSpace(c.PostForm(paramTitle))
	if title == "" {
		title = strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))
	}
	job.Title = &title

	if err := h.jobRepo.Create(c.Request.Context(), &job); err != nil {
		_ = h.fileService.RemoveFile(filePath)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create job"})
		return
	}
	if err := h.taskQueue.EnqueueJob(jobID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to enqueue job"})
		return
	}
	c.JSON(http.StatusOK, job)
}

// @Summary List projects
// @Description Project folders under Projects/ in Nextcloud
// @Tags memos
// @Produce json
// @Success 200 {object} map[string][]string
// @Router /api/v1/projects [get]
// @Security ApiKeyAuth
// @Security BearerAuth
func (h *Handler) ListProjects(c *gin.Context) {
	projects, err := transcription.PrefectProjects(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Projects unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"projects": projects})
}

// @Summary Create a project
// @Description Create a project folder under Projects/ in Nextcloud
// @Tags memos
// @Accept json
// @Produce json
// @Success 200 {object} map[string][]string
// @Router /api/v1/projects [post]
// @Security ApiKeyAuth
// @Security BearerAuth
func (h *Handler) CreateProject(c *gin.Context) {
	var body struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	name := strings.TrimSpace(body.Name)
	if !projectNamePattern.MatchString(name) || strings.Trim(name, ". ") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid project name"})
		return
	}
	projects, err := transcription.PrefectCreateProject(c.Request.Context(), name)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "Project could not be created"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"projects": projects})
}

// @Summary Memo status
// @Description Transcription status plus the Nextcloud publication status of a memo
// @Tags memos
// @Produce json
// @Param id path string true "Job ID"
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/memos/{id} [get]
// @Security ApiKeyAuth
// @Security BearerAuth
func (h *Handler) GetMemo(c *gin.Context) {
	job, err := h.jobRepo.FindByID(c.Request.Context(), c.Param("id"))
	if err != nil || job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Memo not found"})
		return
	}
	reply := gin.H{"id": job.ID, "status": job.Status, "memo": job.Memo}
	if job.ErrorMessage != nil {
		reply["error"] = *job.ErrorMessage
	}
	if id := bridgeJobID(job.Transcript); job.Status == models.StatusCompleted && id != "" && job.Memo.Project != "" {
		if publication, err := transcription.PrefectPublication(c.Request.Context(), id); err == nil {
			reply["publication"] = publication
		}
	}
	c.JSON(http.StatusOK, reply)
}

// bridgeJobID reads the audio bridge handoff id from the stored transcript metadata.
func bridgeJobID(transcript *string) string {
	if transcript == nil {
		return ""
	}
	var stored struct {
		Metadata map[string]string `json:"metadata"`
	}
	if json.Unmarshal([]byte(*transcript), &stored) != nil {
		return ""
	}
	return stored.Metadata["bridge_job_id"]
}

// @Summary Live text for a recording in progress
// @Description Transcribe one short audio chunk (any format ffmpeg reads, e.g. Opus in Ogg; max 4 MB) while recording; the provider is MEMORECORDER_LIVE_PROVIDER
// @Tags memos
// @Accept multipart/form-data
// @Produce json
// @Param audio formData file true "Audio chunk (e.g. Opus in Ogg)"
// @Param language formData string false "nl, en or auto (default)"
// @Success 200 {object} transcription.LiveResult
// @Router /api/v1/live [post]
// @Security ApiKeyAuth
// @Security BearerAuth
func (h *Handler) LiveTranscribe(c *gin.Context) {
	language := getFormValueWithDefault(c, "language", "auto")
	if language != "nl" && language != "en" && language != "auto" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "language must be nl, en or auto"})
		return
	}
	header, err := c.FormFile(paramAudio)
	if err != nil || header.Size <= 0 || header.Size > 4*1024*1024 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Audio chunk (max 4 MB) is required"})
		return
	}
	file, err := header.Open()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unreadable audio chunk"})
		return
	}
	defer file.Close()
	audio, err := io.ReadAll(io.LimitReader(file, 4*1024*1024))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Unreadable audio chunk"})
		return
	}
	provider, err := transcription.NewLiveProvider()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
		return
	}
	result, err := provider.Transcribe(c.Request.Context(), audio, language)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Live transcription unavailable"})
		return
	}
	c.JSON(http.StatusOK, result)
}
