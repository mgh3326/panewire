package pacelever

// Golden generator, copied into a throwaway pacelever clone by regen.sh: it
// runs pacelever's own gate (the CLI's --account-key flow) over panewire's
// admission scenarios and records the verdicts. Never committed to pacelever.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type pwScenario struct {
	Name    string                     `json:"name"`
	Now     time.Time                  `json:"now"`
	Request map[string]string          `json:"request"`
	Config  string                     `json:"config"`
	Catalog []map[string]any           `json:"catalog"`
	Latest  SharedLatest               `json:"latest"`
	Rest    map[string]json.RawMessage `json:"-"`
}

type pwVerdict struct {
	OK           bool     `json:"ok"`
	ExitCode     int      `json:"exit_code"`
	Reason       string   `json:"reason"`
	Pool         string   `json:"provider_id"`
	Basis        string   `json:"gate_basis"`
	Missing      []string `json:"missing_windows"`
	Alternatives []string `json:"alternatives"`
	Receipt      bool     `json:"receipt"`
}

func pwGate(t *testing.T, s pwScenario) pwVerdict {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(s.Config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", path)
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("%s: config: %v", s.Name, err)
	}
	rows, err := decodeCatalog(s.Catalog)
	if err != nil {
		t.Fatalf("%s: catalog: %v", s.Name, err)
	}
	base := BundledCatalog()
	c := Catalog{Rows: rows, Info: CatalogInfo{Source: BenchServer}, Defaults: base.Defaults, Aliases: base.Aliases}
	r := s.Request
	req := Request{Profile: r["profile"], Effort: r["effort"], Role: r["role"], Purpose: r["purpose"], OperatorRequest: r["operator_request"], RequestedBy: r["requested_by"], E6Arm: r["e6_arm"]}
	key := r["account_key"]
	in := Inputs{Catalog: c, Config: cfg, Now: s.Now}
	d := Gate(in, req)
	pool := key[:len(key)-17]
	if pool != d.ProviderID {
		return pwVerdict{ExitCode: Invalid, Reason: "--account-key does not match profile pool"}
	}
	if IsCreditPool(pool) && (d.OK || d.ExitCode == Unmeasurable) {
		m := newManualStore()
		for _, x := range sharedManual(s.Latest.Manual, pool) {
			m.History = append(m.History, x)
			m.Latest[x.Pool+":"+x.Window] = x.ID
		}
		in.Manual = m
		in.AccountScoped = true
		d = Gate(in, req)
	} else if d.OK || d.ExitCode == Unmeasurable {
		o, ok := sharedAuto(s.Latest.Observation, pool, key, s.Now, cfg.Quota.freshTTL(pool), false)
		if !ok {
			msg := msgAccountStale
			if s.Latest.Observation != nil {
				if _, well := sharedAuto(s.Latest.Observation, pool, key, s.Now, 1<<62, true); !well {
					msg = msgAccountMalformed
				}
			}
			return pwVerdict{ExitCode: Unmeasurable, Reason: msg}
		}
		o.PoolClass = cfg.Pool(pool, s.Now).Class
		in.Observations = []Observation{o}
		in.AccountScoped = true
		d = Gate(in, req)
	}
	return pwVerdict{OK: d.OK, ExitCode: d.ExitCode, Reason: d.Reason, Pool: d.ProviderID, Basis: d.GateBasis, Missing: d.MissingWindows, Alternatives: d.Alternatives, Receipt: true}
}

func TestPanewireAdmissionGolden(t *testing.T) {
	in := os.Getenv("PW_SCENARIOS")
	if in == "" {
		t.Skip("PW_SCENARIOS unset")
	}
	data, err := os.ReadFile(in)
	if err != nil {
		t.Fatal(err)
	}
	var raws []map[string]json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		t.Fatal(err)
	}
	for i, raw := range raws {
		var s pwScenario
		b, _ := json.Marshal(raw)
		if err := json.Unmarshal(b, &s); err != nil {
			t.Fatalf("scenario %d: %v", i, err)
		}
		v := pwGate(t, s)
		out, _ := json.Marshal(v)
		raw["pacelever"] = out
	}
	out, _ := json.MarshalIndent(raws, "", "  ")
	if err := os.WriteFile(os.Getenv("PW_OUT"), append(out, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
