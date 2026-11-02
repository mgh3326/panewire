package fleet

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ompAgent reads only user-agent frontmatter, never OMP auth or settings.
// Glob order makes duplicate model matches deterministic.
func (a *App) ompAgent(model string) string {
	paths, _ := filepath.Glob(filepath.Join(a.Home, ".omp", "agent", "agents", "*.md"))
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		name, agentModel := ompAgentFrontmatter(path)
		if name != "" && agentModel == model {
			return name
		}
	}
	return ""
}

func ompAgentFrontmatter(path string) (name, model string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	if !s.Scan() || strings.TrimSpace(s.Text()) != "---" {
		return "", ""
	}
	seen := map[string]bool{}
	for s.Scan() {
		line := s.Text()
		if strings.TrimSpace(line) == "---" {
			return name, model
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok || key != "name" && key != "model" {
			continue
		}
		if seen[key] {
			return "", ""
		}
		seen[key] = true
		value, ok = ompAgentScalar(strings.TrimSpace(value))
		if !ok {
			return "", ""
		}
		if key == "name" {
			name = value
		} else {
			model = value
		}
	}
	return "", ""
}

func ompAgentScalar(value string) (string, bool) {
	if value == "" || value[0] != '"' && value[0] != '\'' {
		value, _, _ = strings.Cut(value, " #")
		return strings.TrimSpace(value), true
	}
	quote := value[0]
	for i := 1; i < len(value); i++ {
		if quote == '"' && value[i] == '\\' {
			i++
			continue
		}
		if value[i] != quote {
			continue
		}
		if quote == '\'' && i+1 < len(value) && value[i+1] == '\'' {
			i++
			continue
		}
		if rest := strings.TrimSpace(value[i+1:]); rest != "" && !strings.HasPrefix(rest, "#") {
			return "", false
		}
		if quote == '\'' {
			return strings.ReplaceAll(value[1:i], "''", "'"), true
		}
		parsed, err := strconv.Unquote(value[:i+1])
		return parsed, err == nil
	}
	return "", false
}
