package api

import (
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
	if m.MeetingType != "meeting" && m.MeetingType != "interview" {
		return m, "meeting_type must be meeting or interview"
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
// @Param meeting_type formData string false "meeting (default) or interview"
// @Param speakers formData int false "Number of people"
// @Param language formData string false "nl (default) or en"
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
	if language != "nl" && language != "en" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "language must be nl or en"})
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
