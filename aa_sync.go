package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

const aaKeyName = "ARTIFICIAL_ANALYSIS_API_KEY"
const aaURL = "https://artificialanalysis.ai/api/v2/language/models/free"

type aaRowKey struct{ Profile, ModelID, Effort string }
type aaModel struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Evaluations struct {
		Intelligence *float64 `json:"artificial_analysis_intelligence_index"`
	} `json:"evaluations"`
	Pricing struct {
		Input  *float64 `json:"price_1m_input_tokens"`
		Output *float64 `json:"price_1m_output_tokens"`
	} `json:"pricing"`
	Performance struct {
		Speed *float64 `json:"median_output_tokens_per_second"`
	} `json:"performance"`
}
type aaPage struct {
	Version    int `json:"intelligence_index_version"`
	Pagination struct {
		Page    int   `json:"page"`
		HasMore *bool `json:"has_more"`
	} `json:"pagination"`
	Data []aaModel `json:"data"`
}
type aaAnnotation struct {
	Intelligence *float64 `json:"intelligence_index"`
	Input        *float64 `json:"price_1m_input_tokens"`
	Output       *float64 `json:"price_1m_output_tokens"`
	Speed        *float64 `json:"median_output_tokens_per_second"`
	Name         string   `json:"aa_name"`
	FetchedAt    string   `json:"fetched_at"`
}
type aaSync struct {
	endpoint string
	key      string
	now      func() time.Time
	client   *http.Client
}

func (a aaSync) fetch(ctx context.Context) ([]aaModel, int, error) {
	if strings.TrimSpace(a.key) == "" {
		return nil, 0, fmt.Errorf("%s is required", aaKeyName)
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if a.client != nil {
		client.Transport = a.client.Transport
	}
	var models []aaModel
	version := 0
	for page := 1; page <= 20; page++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.endpoint, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid Artificial Analysis request")
		}
		q := req.URL.Query()
		q.Set("page", fmt.Sprint(page))
		req.URL.RawQuery = q.Encode()
		req.Header.Set("x-api-key", a.key)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "panewire")
		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, fmt.Errorf("Artificial Analysis request failed")
		}
		if resp.StatusCode != http.StatusOK {
			status := resp.StatusCode
			retry := resp.Header.Get("Retry-After")
			resp.Body.Close()
			if status == http.StatusForbidden {
				return nil, 0, fmt.Errorf("Artificial Analysis: tier does not allow this route")
			}
			if status == http.StatusTooManyRequests {
				if strings.Contains(retry, a.key) {
					retry = "withheld"
				}
				return nil, 0, fmt.Errorf("Artificial Analysis HTTP 429: Retry-After %s", retry)
			}
			return nil, 0, fmt.Errorf("Artificial Analysis HTTP %d", status)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1))
		resp.Body.Close()
		if err != nil || len(data) > 16<<20 {
			return nil, 0, fmt.Errorf("invalid Artificial Analysis model data")
		}
		var p aaPage
		if json.Unmarshal(data, &p) != nil || p.Version <= 0 || p.Pagination.Page != page || p.Pagination.HasMore == nil || len(p.Data) == 0 {
			return nil, 0, fmt.Errorf("invalid Artificial Analysis model data")
		}
		if version != 0 && version != p.Version {
			return nil, 0, fmt.Errorf("Artificial Analysis index version changed during fetch")
		}
		version = p.Version
		for _, m := range p.Data {
			if m.ID == "" || m.Name == "" {
				return nil, 0, fmt.Errorf("invalid Artificial Analysis model identity")
			}
		}
		models = append(models, p.Data...)
		if !*p.Pagination.HasMore {
			return models, version, nil
		}
	}
	return nil, 0, fmt.Errorf("Artificial Analysis page limit exceeded")
}

func aaAnnotate(rows []map[string]any, models []aaModel, version int, at time.Time) ([]map[string]any, int, []string, int) {
	byID := map[string][]aaModel{}
	for _, m := range models {
		byID[m.ID] = append(byID[m.ID], m)
	}
	result := make([]map[string]any, 0, len(rows))
	var unmapped []string
	annotated, changed := 0, 0
	for _, original := range rows {
		row := make(map[string]any, len(original))
		for k, v := range original {
			row[k] = v
		}
		str := func(k string) string { s, _ := row[k].(string); return s }
		key := aaRowKey{str("profile"), str("model_id"), str("effort")}
		id, known := aaMappings[key]
		matches := byID[id]
		if !known || len(matches) != 1 {
			reason := "no explicit mapping"
			if known {
				reason = "AA variant absent or ambiguous"
			}
			unmapped = append(unmapped, fmt.Sprintf("unmapped %s@%s model=%s reason=%s", key.Profile, key.Effort, key.ModelID, reason))
			result = append(result, row)
			continue
		}
		m := matches[0]
		speed := m.Performance.Speed
		if speed != nil && *speed == 0 {
			speed = nil
		}
		clean := func(n *float64) *float64 {
			if n != nil && *n < 0 {
				return nil
			}
			return n
		}
		annotation := aaAnnotation{clean(m.Evaluations.Intelligence), clean(m.Pricing.Input), clean(m.Pricing.Output), clean(speed), m.Name, at.UTC().Format(time.RFC3339Nano)}
		source := fmt.Sprintf("artificialanalysis:v2:ii%d:%s", version, at.UTC().Format(time.DateOnly))
		var old aaAnnotation
		_ = json.Unmarshal([]byte(str("benchmark_annotation")), &old)
		old.FetchedAt = annotation.FetchedAt
		if str("benchmark_source") != source || !reflect.DeepEqual(old, annotation) {
			changed++
		}
		b, _ := json.Marshal(annotation)
		row["benchmark_source"] = source
		row["benchmark_annotation"] = string(b)
		annotated++
		result = append(result, row)
	}
	sort.Strings(unmapped)
	return result, annotated, unmapped, changed
}

func aaCatalogArtifact(data []byte, rows []map[string]any) ([]byte, error) {
	var original struct {
		Catalog []map[string]json.RawMessage `json:"catalog"`
	}
	if err := json.Unmarshal(data, &original); err != nil || len(original.Catalog) != len(rows) {
		return nil, fmt.Errorf("invalid catalog")
	}
	var out bytes.Buffer
	out.WriteString("{\n  \"catalog\": [\n")
	for i, row := range original.Catalog {
		if i > 0 {
			out.WriteString(",\n")
		}
		for _, field := range []string{"benchmark_source", "benchmark_annotation"} {
			if value, ok := rows[i][field]; ok {
				b, err := json.Marshal(value)
				if err != nil {
					return nil, err
				}
				row[field] = b
			}
		}
		keys := make([]string, 0, len(row))
		for key := range row {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteString("    {")
		for j, key := range keys {
			if j > 0 {
				out.WriteByte(',')
			}
			out.WriteString("\n      ")
			k, err := json.Marshal(key)
			if err != nil {
				return nil, err
			}
			out.Write(k)
			out.WriteString(": ")
			out.Write(row[key])
		}
		out.WriteString("\n    }")
	}
	out.WriteString("\n  ]\n}\n")
	return out.Bytes(), nil
}

func runAASync(args []string, out, errOut io.Writer) int {
	var path string
	dry := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--dry-run":
			dry = true
		case "--out":
			i++
			if i >= len(args) || args[i] == "" || strings.HasPrefix(args[i], "-") {
				return ExitUsage
			}
			path = args[i]
		default:
			return ExitUsage
		}
	}
	key := os.Getenv(aaKeyName)
	if key == "" {
		if config, err := os.UserConfigDir(); err == nil {
			p := filepath.Join(config, "panewire", "aa.env")
			if _, err := os.Lstat(p); err == nil {
				values, e := loadMode0600Env(p)
				if e != nil {
					fmt.Fprintln(errOut, "aa-sync: invalid env file")
					return ExitUsage
				}
				key = values[aaKeyName]
			}
		}
	}
	if strings.TrimSpace(key) == "" {
		fmt.Fprintln(errOut, aaKeyName, "is required")
		return ExitUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "pacelever", "bench", "catalog", "list", "--json")
	data, err := cmd.Output()
	if err != nil {
		fmt.Fprintln(errOut, "aa-sync: cannot read catalog")
		return ExitInternal
	}
	var doc struct {
		Catalog []map[string]any `json:"catalog"`
		Source  struct {
			Source string `json:"source"`
			Stale  bool   `json:"stale"`
		} `json:"source"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if dec.Decode(&doc) != nil || len(doc.Catalog) == 0 || doc.Source.Stale || (path != "" && doc.Source.Source != "server") {
		fmt.Fprintln(errOut, "aa-sync: requires a fresh server catalog for --out")
		return ExitInternal
	}
	sync := aaSync{endpoint: aaURL, key: key, now: time.Now}
	models, version, err := sync.fetch(context.Background())
	if err != nil {
		fmt.Fprintln(errOut, err)
		return ExitInternal
	}
	rows, annotated, unmapped, changed := aaAnnotate(doc.Catalog, models, version, sync.now())
	if path != "" && !dry {
		b, e := aaCatalogArtifact(data, rows)
		if e != nil || replaceFileAtomically(path, ".aa-sync-*", b, 0600) != nil {
			fmt.Fprintln(errOut, "aa-sync: cannot write catalog artifact")
			return ExitInternal
		}
	}
	for _, row := range rows {
		if source, ok := row["benchmark_source"].(string); ok && strings.HasPrefix(source, "artificialanalysis:v2:") {
			fmt.Fprintf(out, "%s@%s model=%s benchmark_source=%s benchmark_annotation=%s\n", row["profile"], row["effort"], row["model_id"], source, row["benchmark_annotation"])
		}
	}
	for _, line := range unmapped {
		fmt.Fprintln(out, line)
	}
	fmt.Fprintf(out, "aa-sync annotated=%d unmapped=%d changed=%d dry_run=%t\nData: Artificial Analysis https://artificialanalysis.ai/\n", annotated, len(unmapped), changed, dry)
	return 0
}
