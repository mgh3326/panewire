package panewire

import "regexp"

var (
	hubQuotaPoolPattern      = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	hubQuotaAccountFPPattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
)
