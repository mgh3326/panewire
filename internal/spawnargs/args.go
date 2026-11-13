// Package spawnargs holds the one rule for the flags a hub job.spawn may
// carry. The node applies it to every job.spawn it receives, and the submitter
// applies the same rule to the args the hub will send, so a request the node
// would drop is refused before it leaves with the flag named.
package spawnargs

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/grade"
)

const (
	// MaxValueBytes bounds an ordinary flag value on the wire.
	MaxValueBytes = 200
	// MaxPathBytes bounds the operator's -c and -p values, which are node
	// paths rather than names.
	MaxPathBytes = 4096
	// valueCharset is the class of characters an ordinary value may use. It
	// is stated once here and quoted in every refusal that names it.
	valueCharset = `[A-Za-z0-9._:/@-]`
)

var (
	// ValuePattern admits one ordinary flag value: the same characters the
	// hub uses for a cwd key or a pool name.
	ValuePattern         = regexp.MustCompile(`^` + valueCharset + `{1,200}$`)
	tagsPattern          = regexp.MustCompile(`^[A-Za-z0-9._:/@-]+(,[A-Za-z0-9._:/@-]+){0,15}$`)
	excludedHostsPattern = regexp.MustCompile(`^[A-Za-z0-9._:/@-]+(,[A-Za-z0-9._:/@-]+)*$`)
	gradePattern         = regexp.MustCompile(`^(` + strings.Join(quotedGrades(), `|`) + `)$`)
	docKeyPattern        = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
	jobIDPattern         = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
)

func quotedGrades() []string {
	grades := grade.Grades()
	for i := range grades {
		grades[i] = regexp.QuoteMeta(grades[i])
	}
	return grades
}

func gradeDescription() string {
	grades := grade.Grades()
	return strings.Join(grades[:len(grades)-1], ", ") + " or " + grades[len(grades)-1]
}

var baseFlags = []string{"-m", "-w", "-l", "--t", "--effort", "--e6-arm", "--job", "--owner", "--report", "--report-relative", "--role", "--lane", "--parent", "-L", "--task", "--parent-job", "--tags", "--urgency", "--author-family", "--builder-job", "--review-pass"}

var operatorFlags = []string{"-c", "-p", "--host", "--exclude-host", "--exclude-pool", "--grade", "--override-reason", "--containment", "--operator-request", "--requested-by", "--purpose"}

var operatorBooleans = []string{"--keep", "--job-dup-ok", "--task-hk-bypass", "--allow-escalation"}

// Refusal returns why the node would refuse these job.spawn args, or "" when
// it would accept them. operator selects the flag set an operator-authorized
// spawn may use. The sentence names the flag and the rule it broke.
func Refusal(args []string, operator bool) string {
	allowed := map[string]bool{}
	for _, flag := range baseFlags {
		allowed[flag] = true
	}
	booleans := map[string]bool{}
	if operator {
		for _, flag := range operatorFlags {
			allowed[flag] = true
		}
		for _, flag := range operatorBooleans {
			booleans[flag] = true
		}
	}
	seen := map[string]bool{}
	for i := 0; i < len(args); {
		flag := args[i]
		if seen[flag] {
			return fmt.Sprintf("duplicate %s: the node accepts each flag once", flag)
		}
		seen[flag] = true
		if booleans[flag] {
			i++
			continue
		}
		if !allowed[flag] {
			return fmt.Sprintf("%s is not a flag the node accepts for a hub spawn", flag)
		}
		if i+1 >= len(args) {
			return fmt.Sprintf("%s needs a value", flag)
		}
		value := args[i+1]
		if reason := valueRefusal(flag, value, operator); reason != "" {
			return reason
		}
		i += 2
	}
	return ""
}

func valueRefusal(flag, value string, operator bool) string {
	path := operator && (flag == "-c" || flag == "-p")
	maxBytes := MaxValueBytes
	if path {
		maxBytes = MaxPathBytes
	}
	if len(value) > maxBytes {
		return fmt.Sprintf("%s value is longer than %d bytes", flag, maxBytes)
	}
	if value == "" {
		return fmt.Sprintf("%s value is empty", flag)
	}
	if !utf8.ValidString(value) {
		return fmt.Sprintf("%s value is not valid UTF-8", flag)
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		return fmt.Sprintf("%s value contains a control character", flag)
	}
	freeText := operator && (path || flag == "--override-reason" || flag == "--purpose" || flag == "-w")
	if freeText {
		return ""
	}
	switch flag {
	case "--tags":
		if !tagsPattern.MatchString(value) {
			return fmt.Sprintf("--tags value must be comma-separated values of %s, at most 16", valueCharset)
		}
	case "--grade":
		if !gradePattern.MatchString(value) {
			return "--grade value must be " + gradeDescription()
		}
	case "--exclude-host", "--exclude-pool":
		if !excludedHostsPattern.MatchString(value) {
			return fmt.Sprintf("%s value must be comma-separated values of %s", flag, valueCharset)
		}
	default:
		if !ValuePattern.MatchString(value) {
			return fmt.Sprintf("%s value has a character outside %s", flag, valueCharset)
		}
	}
	return argValueRefusal(flag, value, operator)
}

// argValueRefusal is the per-flag rule that the pattern alone cannot state.
func argValueRefusal(flag, value string, operator bool) string {
	switch flag {
	case "--role":
		if value == "implementer" || value == "reviewer" || operator && value == "consult" {
			return ""
		}
		return "--role value must be implementer or reviewer"
	case "--author-family":
		switch value {
		case "anthropic", "openai", "google", "xai", "deepseek", "meta", "mistral", "other":
			return ""
		}
		return "--author-family value must be anthropic, openai, google, xai, deepseek, meta, mistral or other"
	case "--review-pass":
		if len(value) == 1 && value[0] >= '1' && value[0] <= '9' {
			return ""
		}
		return "--review-pass value must be a single digit from 1 to 9"
	case "--parent-job", "--builder-job":
		if jobIDPattern.MatchString(value) {
			return ""
		}
		return fmt.Sprintf("%s value must be 1-128 characters of letters, digits, dot, underscore or hyphen", flag)
	case "--report-relative":
		if ValidDocKey(value) {
			return ""
		}
		return "--report-relative value must be a relative path of letters, digits and ._/-, with no empty, \".\" or \"..\" segments"
	}
	return ""
}

// ValidDocKey admits a brief document key or a path relative to the node's
// jobs root: a slash-separated list of non-empty segments that are neither
// "." nor "..", at most 200 characters.
func ValidDocKey(key string) bool {
	if len(key) == 0 || len(key) > 200 || !docKeyPattern.MatchString(key) || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
