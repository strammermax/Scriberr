package api

import "testing"

func TestBridgeJobIDFromTranscript(t *testing.T) {
	stored := `{"text":"x","metadata":{"bridge_job_id":"0123456789abcdef0123456789abcdef"}}`
	if bridgeJobID(&stored) != "0123456789abcdef0123456789abcdef" {
		t.Fatal("handoff id not found")
	}
	broken := "not json"
	if bridgeJobID(&broken) != "" || bridgeJobID(nil) != "" {
		t.Fatal("expected empty id")
	}
}

func TestValidMemoSettingsDefaults(t *testing.T) {
	m, problem := validMemoSettings(" Projectx ", "", "", "", "")
	if problem != "" {
		t.Fatal(problem)
	}
	if m.Project != "Projectx" || m.MeetingType != "meeting" || m.Outputs != "plain,verslag,samenvatting" || m.RecordedAt != nil {
		t.Fatalf("bad defaults: %+v", m)
	}
}

func TestValidMemoSettingsChoices(t *testing.T) {
	if _, problem := validMemoSettings("A", "conversation", "", "", ""); problem != "" {
		t.Fatal(problem)
	}
	m, problem := validMemoSettings("Klant A", "interview", "3", "verslag, samenvatting", "2026-10-12T09:30:00+02:00")
	if problem != "" {
		t.Fatal(problem)
	}
	if m.Speakers != 3 || m.Outputs != "verslag,samenvatting" || m.RecordedAt == nil {
		t.Fatalf("bad settings: %+v", m)
	}
}

func TestValidMemoSettingsRejects(t *testing.T) {
	for _, c := range [][5]string{
		{"../etc", "", "", "", ""},
		{"", "", "", "", ""},
		{"..", "", "", "", ""},
		{"A", "party", "", "", ""},
		{"A", "", "0", "", ""},
		{"A", "", "", "notulen", ""},
		{"A", "", "", "", "gisteren"},
	} {
		if _, problem := validMemoSettings(c[0], c[1], c[2], c[3], c[4]); problem == "" {
			t.Errorf("accepted %v", c)
		}
	}
}
