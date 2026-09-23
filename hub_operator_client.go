package panewire

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// hubOperatorProductionHost is the single origin production operator commands
// may send hub credentials to. It is deliberately not a flag: the approved
// host is fixed code so a mistyped or injected --hub-url cannot carry the
// operator bearer (or Cloudflare Access pair) to another authority.
const hubOperatorProductionHost = "hub.robinco.dev"

const hubOperatorClientTimeout = 15 * time.Second

var (
	errHubOperatorRedirect = errors.New("hub request redirect rejected")
	errHubOperatorRequest  = errors.New("hub request rejected")
)

// hubOperatorAPIPaths is the closed set of fixed command paths a credentialled
// operator request may target. Anything else — including a mutated request
// URL reaching the transport — fails closed.
var hubOperatorAPIPaths = map[string]bool{
	"/v1/nodes":           true,
	"/v1/jobs":            true,
	"/v1/jobs/orphaned":   true,
	"/v1/jobs/reassign":   true,
	"/v1/lanes":           true,
	"/v1/placement":       true,
	"/v1/placement/slots": true,
	"/v1/burst":           true,
	"/v1/burst/request":   true,
	"/v1/burst/release":   true,
	"/v1/burst/holds":     true,
	"/v1/update":          true,
}

func validHubOperatorAPIPath(path string) bool {
	if hubOperatorAPIPaths[path] {
		return true
	}
	if lane, found := strings.CutPrefix(path, "/v1/lanes/"); found {
		return laneNamePattern.MatchString(lane)
	}
	return false
}

// parseHubOperatorBase validates a --hub-url value and returns the normalized
// origin. Production admits exactly https://hub.robinco.dev with effective
// port 443; a scheme, host, port, userinfo, query, fragment, path, or
// authority decoration that differs from that one origin is rejected. Tests
// may inject an explicit http or https loopback origin only through
// allowInsecureForTests, a dependency no production CLI flag can set.
func parseHubOperatorBase(raw string, allowInsecureForTests bool) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.Opaque != "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawPath != "" ||
		(parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("invalid hub URL")
	}
	hostname := parsed.Hostname()
	port := parsed.Port()
	// The hostname must already be in canonical lowercase ASCII form: a
	// trailing dot, mixed case, or an IDNA/percent-decorated label is treated
	// as authority confusion, not normalized away.
	if hostname == "" || hostname != strings.ToLower(hostname) || strings.HasSuffix(hostname, ".") {
		return nil, errors.New("invalid hub URL")
	}
	for _, character := range hostname {
		if character > 0x7f {
			return nil, errors.New("invalid hub URL")
		}
	}
	if port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 || strconv.Itoa(number) != port {
			return nil, errors.New("invalid hub URL")
		}
	}
	// The authority must be exactly the canonical hostname[:port] form. Any
	// surviving decoration (semicolon parameters, encoded bytes, extra
	// colons) turns this comparison into a mismatch.
	wantHost := hostname
	if port != "" {
		wantHost = net.JoinHostPort(hostname, port)
	}
	if parsed.Host != wantHost {
		return nil, errors.New("invalid hub URL")
	}
	if parsed.Scheme == "https" && hostname == hubOperatorProductionHost && (port == "" || port == "443") {
		base := *parsed
		base.Path = ""
		return &base, nil
	}
	if !allowInsecureForTests || (parsed.Scheme != "http" && parsed.Scheme != "https") || !hubOperatorLoopbackHost(hostname) {
		return nil, errors.New("invalid hub URL")
	}
	base := *parsed
	base.Path = ""
	return &base, nil
}

// hubOperatorLoopbackHost admits only names that resolve to this machine, so
// an injected test client can never point credentials at a routed peer.
func hubOperatorLoopbackHost(hostname string) bool {
	if hostname == "localhost" {
		return true
	}
	ip := net.ParseIP(hostname)
	return ip != nil && ip.IsLoopback()
}

// hubExpectedURLContextKey stamps the exact request URL the boundary
// validated; the transport compares it again immediately before the wire.
type hubExpectedURLContextKey struct{}

// hubGuardedTransport is the last-moment guard inside the dedicated client. A
// request that did not come through this boundary carries no stamped URL, and
// a request whose URL was mutated after stamping no longer matches: both fail
// closed before the base transport can send credentials anywhere.
type hubGuardedTransport struct{ base http.RoundTripper }

func (transport hubGuardedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	expected, _ := request.Context().Value(hubExpectedURLContextKey{}).(string)
	if expected == "" || request.URL == nil || request.URL.String() != expected {
		return nil, errHubOperatorRequest
	}
	base := transport.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(request)
}

// hubOperatorClient is the one boundary every operator-token hub request
// crosses. It owns a dedicated http.Client — never the package-default — with
// a finite timeout and a CheckRedirect that rejects every redirect, so a
// credentialled request can never be re-aimed by a 30x or by a future caller
// forgetting client hardening.
type hubOperatorClient struct {
	base   *url.URL
	client *http.Client
	token  string
	cf     hubCFAccessEnv
}

// newHubOperatorClient validates the hub URL and binds the operator
// credential to a dedicated client. Only the injected client's RoundTripper
// is honored (a fixture transport or TLS dialer); its own CheckRedirect,
// timeout, and jar are not — the boundary owns those controls.
func newHubOperatorClient(rawURL, token string, cf hubCFAccessEnv, deps hubCLIDeps, timeout time.Duration) (*hubOperatorClient, error) {
	base, err := parseHubOperatorBase(rawURL, deps.AllowInsecureForTests)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = hubOperatorClientTimeout
	}
	var transport http.RoundTripper = http.DefaultTransport
	if deps.HTTPClient != nil && deps.HTTPClient.Transport != nil {
		transport = deps.HTTPClient.Transport
	}
	return &hubOperatorClient{
		base: base,
		client: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errHubOperatorRedirect
			},
			Transport: hubGuardedTransport{base: transport},
		},
		token: token,
		cf:    cf,
	}, nil
}

// buildRequest mints a request for one fixed API path, validates the final
// URL, stamps it into the request context for the transport-side check, and
// only then attaches the operator bearer and optional Cloudflare Access
// headers. Credentials therefore never exist on a request whose URL has not
// already passed every check.
func (client *hubOperatorClient) buildRequest(ctx context.Context, method, apiPath string, query url.Values, body io.Reader) (*http.Request, error) {
	if !validHubOperatorAPIPath(apiPath) {
		return nil, errors.New("invalid hub API path")
	}
	target := *client.base
	target.Path = apiPath
	if query != nil {
		target.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	if request.URL == nil || request.URL.Scheme != client.base.Scheme || request.URL.Host != client.base.Host ||
		request.URL.User != nil || request.URL.Path != apiPath || request.URL.Fragment != "" {
		return nil, errors.New("invalid hub request URL")
	}
	request = request.WithContext(context.WithValue(request.Context(), hubExpectedURLContextKey{}, request.URL.String()))
	request.Header.Set(hubAuthorizationHeader, "Bearer "+client.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if client.cf.ClientID != "" {
		request.Header.Set("CF-Access-Client-Id", client.cf.ClientID)
		request.Header.Set("CF-Access-Client-Secret", client.cf.ClientSecret)
	}
	return request, nil
}

func (client *hubOperatorClient) do(ctx context.Context, method, apiPath string, query url.Values, body io.Reader) (*http.Response, error) {
	request, err := client.buildRequest(ctx, method, apiPath, query, body)
	if err != nil {
		return nil, err
	}
	return client.client.Do(request)
}
