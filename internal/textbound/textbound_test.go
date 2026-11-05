package textbound

import "testing"

func TestPrefixWithinNeverSplitsARune(t *testing.T) {
	for _, tc := range []struct {
		value string
		limit int
		want  string
	}{
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},
		{"가나다", 8, "가나"},
		{"가", 2, ""},
		{"", 0, ""},
	} {
		if got := PrefixWithin(tc.value, tc.limit); got != tc.want {
			t.Errorf("PrefixWithin(%q, %d)=%q, want %q", tc.value, tc.limit, got, tc.want)
		}
	}
}

func TestSuffixWithinNeverSplitsARune(t *testing.T) {
	for _, tc := range []struct {
		value string
		limit int
		want  string
	}{
		{"abc", 5, "abc"},
		{"abcd", 2, "cd"},
		{"가나다", 8, "나다"},
		{"가", 2, ""},
		{"abc", -1, ""},
	} {
		if got := SuffixWithin(tc.value, tc.limit); got != tc.want {
			t.Errorf("SuffixWithin(%q, %d)=%q, want %q", tc.value, tc.limit, got, tc.want)
		}
	}
}

func TestRunePrefix(t *testing.T) {
	for _, tc := range []struct {
		value string
		limit int
		want  string
	}{
		{"가나다", 3, "가나다"},
		{"가나다", 2, "가나"},
		{"abc", 0, ""},
		{"abc", -1, ""},
	} {
		if got := RunePrefix(tc.value, tc.limit); got != tc.want {
			t.Errorf("RunePrefix(%q, %d)=%q, want %q", tc.value, tc.limit, got, tc.want)
		}
	}
}
