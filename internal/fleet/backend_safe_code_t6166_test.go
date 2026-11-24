package fleet

import "testing"

func TestBackendErrorSafeCodeKeepsOnlyBareIdentifiers_t6166(t *testing.T) {
	cases := map[string]string{
		"job_host_forbidden":                "job_host_forbidden",
		"":                                  "",
		"Job Host Forbidden":                "",
		"job_host_forbidden\n token=secret": "",
		"code=leaked":                       "",
	}
	for code, want := range cases {
		got := (&BackendError{Status: 403, Code: code}).SafeCode()
		if got != want {
			t.Fatalf("SafeCode(%q)=%q, want %q", code, got, want)
		}
	}
}
