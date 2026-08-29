package panewire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var machineIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

type enrollDeps struct {
	HTTPClient            *http.Client
	AllowInsecureForTests bool
	Now                   func() time.Time
	Random                io.Reader
}

type supabaseAdminEnv struct {
	URL            string
	SecretKey      string
	PublishableKey string
}

type supabaseAdminClient struct {
	baseURL *url.URL
	secret  string
	client  *http.Client
}

type authAdminUser struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type clientCredentialEnv struct {
	URL            string
	MachineID      string
	AccessToken    string
	RefreshToken   string
	PublishableKey string
}

// runEnrollMachineCLI performs the only credential-creation path in this
// repository.  It has no default admin-env or output path: accidental use
// cannot read ~/.config/panewire or write a secret file there.
func runEnrollMachineCLI(args []string, stdout, stderr io.Writer, deps enrollDeps) int {
	fs := flag.NewFlagSet("panewire enroll-machine", flag.ContinueOnError)
	fs.SetOutput(stderr)
	adminEnvPath := fs.String("admin-env", "", "path to 0600 SUPABASE_URL/SUPABASE_SECRET_KEY env file")
	machineID := fs.String("machine-id", "", "stable lowercase machine identity")
	outPath := fs.String("out", "", "explicit mode-0600 client credential env output path")
	confirm := fs.Bool("confirm", false, "perform the admin API mutations")
	revoke := fs.Bool("revoke", false, "disable existing Auth user and revoke registry entry")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *adminEnvPath == "" || *machineID == "" || (!*revoke && *outPath == "") || !machineIDPattern.MatchString(*machineID) {
		return ExitUsage
	}

	if !*confirm {
		if *revoke {
			fmt.Fprintf(stdout, "DRY-RUN: would disable the Auth user and revoke the machine registry entry for machine_id=%s using admin env keys SUPABASE_URL,SUPABASE_SECRET_KEY from %s\n", *machineID, *adminEnvPath)
		} else {
			fmt.Fprintf(stdout, "DRY-RUN: would create or reconcile one Auth user, upsert one machine registry row, and write a mode-0600 client credential file at %s for machine_id=%s; admin env keys: SUPABASE_URL,SUPABASE_SECRET_KEY\n", *outPath, *machineID)
		}
		return ExitOK
	}

	env, err := loadSupabaseAdminEnv(*adminEnvPath)
	if err != nil {
		fmt.Fprintln(stderr, "enrollment failed: admin environment is invalid")
		return ExitConditionInvalid
	}
	client, err := newSupabaseAdminClient(env, deps)
	if err != nil {
		fmt.Fprintln(stderr, "enrollment failed: Supabase URL is invalid")
		return ExitConditionInvalid
	}

	ctx := context.Background()
	if *revoke {
		if err := revokeMachine(ctx, client, *machineID, nowFor(deps)); err != nil {
			fmt.Fprintln(stderr, "revocation failed")
			return ExitInternal
		}
		fmt.Fprintf(stdout, "CONFIRMED: revoked machine_id=%s; no credential values were printed\n", *machineID)
		return ExitOK
	}

	credential, err := enrollMachine(ctx, client, *machineID, env.PublishableKey, deps)
	if err != nil {
		fmt.Fprintln(stderr, "enrollment failed")
		return ExitInternal
	}
	if err := writeClientCredentialEnv(*outPath, credential); err != nil {
		fmt.Fprintln(stderr, "enrollment failed: client credential file was not written")
		return ExitInternal
	}
	fmt.Fprintf(stdout, "CONFIRMED: enrolled machine_id=%s and wrote mode-0600 client credential file at %s; no credential values were printed\n", *machineID, *outPath)
	return ExitOK
}

func nowFor(deps enrollDeps) time.Time {
	if deps.Now != nil {
		return deps.Now().UTC()
	}
	return time.Now().UTC()
}

func loadSupabaseAdminEnv(path string) (supabaseAdminEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return supabaseAdminEnv{}, err
	}
	result := supabaseAdminEnv{
		URL:            values["SUPABASE_URL"],
		SecretKey:      values["SUPABASE_SECRET_KEY"],
		PublishableKey: values["SUPABASE_PUBLISHABLE_KEY"],
	}
	if result.URL == "" || result.SecretKey == "" {
		return supabaseAdminEnv{}, errors.New("required admin keys missing")
	}
	return result, nil
}

func loadMode0600Env(path string) (map[string]string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0600 {
		return nil, errors.New("env file must be regular mode 0600")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open env file")
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(io.LimitReader(file, 1<<20))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok || key == "" {
			return nil, errors.New("malformed env file")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			decoded, err := strconv.Unquote(value)
			if err != nil {
				return nil, errors.New("malformed quoted env value")
			}
			value = decoded
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("read env file")
	}
	return values, nil
}

func newSupabaseAdminClient(env supabaseAdminEnv, deps enrollDeps) (*supabaseAdminClient, error) {
	u, err := url.Parse(env.URL)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || (u.Scheme != "https" && !deps.AllowInsecureForTests) {
		return nil, errors.New("invalid Supabase URL")
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &supabaseAdminClient{baseURL: u, secret: env.SecretKey, client: client}, nil
}

func (c *supabaseAdminClient) request(ctx context.Context, method, endpoint string, input any, output any, profile string) (int, error) {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return 0, errors.New("encode request")
		}
		body = bytes.NewReader(encoded)
	}
	u := *c.baseURL
	if strings.Contains(endpoint, "?") {
		parts := strings.SplitN(endpoint, "?", 2)
		u.Path = strings.TrimSuffix(u.Path, "/") + parts[0]
		u.RawQuery = parts[1]
	} else {
		u.Path = strings.TrimSuffix(u.Path, "/") + endpoint
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return 0, errors.New("make request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.secret)
	req.Header.Set("apikey", c.secret)
	if profile != "" {
		if method == http.MethodGet || method == http.MethodHead {
			req.Header.Set("Accept-Profile", profile)
		} else {
			req.Header.Set("Content-Profile", profile)
		}
	}
	response, err := c.client.Do(req)
	if err != nil {
		return 0, errors.New("Supabase request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, errors.New("Supabase request rejected")
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return response.StatusCode, nil
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 2<<20))
	if err := decoder.Decode(output); err != nil {
		return response.StatusCode, errors.New("decode Supabase response")
	}
	return response.StatusCode, nil
}

func machineEmail(machineID string) string { return "pw-" + machineID + "@machines.invalid" }

func (c *supabaseAdminClient) findUser(ctx context.Context, email string) (authAdminUser, bool, error) {
	var result struct {
		Users []authAdminUser `json:"users"`
	}
	if _, err := c.request(ctx, http.MethodGet, "/auth/v1/admin/users?page=1&per_page=1000", nil, &result, ""); err != nil {
		return authAdminUser{}, false, err
	}
	for _, user := range result.Users {
		if user.Email == email {
			if _, err := uuid.Parse(user.ID); err != nil {
				return authAdminUser{}, false, errors.New("invalid auth user ID")
			}
			return user, true, nil
		}
	}
	return authAdminUser{}, false, nil
}

func randomEnrollmentPassword(source io.Reader) (string, error) {
	if source == nil {
		source = rand.Reader
	}
	bytes := make([]byte, 32)
	if _, err := io.ReadFull(source, bytes); err != nil {
		return "", errors.New("generate credential")
	}
	return "pw." + base64.RawURLEncoding.EncodeToString(bytes), nil
}

func enrollMachine(ctx context.Context, client *supabaseAdminClient, machineID, publishableKey string, deps enrollDeps) (clientCredentialEnv, error) {
	email := machineEmail(machineID)
	password, err := randomEnrollmentPassword(deps.Random)
	if err != nil {
		return clientCredentialEnv{}, err
	}
	user, found, err := client.findUser(ctx, email)
	if err != nil {
		return clientCredentialEnv{}, err
	}
	if !found {
		var created authAdminUser
		_, err = client.request(ctx, http.MethodPost, "/auth/v1/admin/users", map[string]any{
			"email": email, "password": password, "email_confirm": true,
		}, &created, "")
		if err != nil {
			return clientCredentialEnv{}, err
		}
		user = created
	} else {
		var updated authAdminUser
		_, err = client.request(ctx, http.MethodPut, "/auth/v1/admin/users/"+url.PathEscape(user.ID), map[string]any{
			"password": password, "email_confirm": true, "ban_duration": "none",
		}, &updated, "")
		if err != nil {
			return clientCredentialEnv{}, err
		}
		if updated.ID != "" {
			user = updated
		}
	}
	if _, err := uuid.Parse(user.ID); err != nil || user.Email != "" && user.Email != email {
		return clientCredentialEnv{}, errors.New("invalid auth user response")
	}

	var session struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}
	if _, err := client.request(ctx, http.MethodPost, "/auth/v1/token?grant_type=password", map[string]string{
		"email": email, "password": password,
	}, &session, ""); err != nil || session.AccessToken == "" || session.RefreshToken == "" {
		return clientCredentialEnv{}, errors.New("machine session creation failed")
	}

	row := []map[string]any{{
		"machine_id": machineID, "auth_user_id": user.ID, "state": "active",
		"revoked_at": nil, "updated_at": nowFor(deps),
	}}
	if _, err := client.request(ctx, http.MethodPost, "/rest/v1/machine_registry?on_conflict=machine_id", row, nil, "panewire"); err != nil {
		return clientCredentialEnv{}, err
	}
	return clientCredentialEnv{
		URL: envURL(client), MachineID: machineID, AccessToken: session.AccessToken,
		RefreshToken: session.RefreshToken, PublishableKey: publishableKey,
	}, nil
}

func envURL(client *supabaseAdminClient) string { return client.baseURL.String() }

func revokeMachine(ctx context.Context, client *supabaseAdminClient, machineID string, now time.Time) error {
	user, found, err := client.findUser(ctx, machineEmail(machineID))
	if err != nil {
		return err
	}
	if found {
		if _, err := client.request(ctx, http.MethodPut, "/auth/v1/admin/users/"+url.PathEscape(user.ID), map[string]string{
			"ban_duration": "876000h",
		}, nil, ""); err != nil {
			return err
		}
	}
	query := url.Values{"machine_id": []string{"eq." + machineID}}
	_, err = client.request(ctx, http.MethodPatch, "/rest/v1/machine_registry?"+query.Encode(), map[string]any{
		"state": "revoked", "revoked_at": now, "updated_at": now,
	}, nil, "panewire")
	return err
}

func writeClientCredentialEnv(path string, credential clientCredentialEnv) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return errors.New("resolve output path")
	}
	if info, err := os.Lstat(abs); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return errors.New("output path is not a regular file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("inspect output path")
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
		return errors.New("create output directory")
	}
	lines := []string{
		"PANEWIRE_SUPABASE_URL=" + strconv.Quote(credential.URL),
		"PANEWIRE_MACHINE_ID=" + strconv.Quote(credential.MachineID),
		"PANEWIRE_SUPABASE_ACCESS_TOKEN=" + strconv.Quote(credential.AccessToken),
		"PANEWIRE_SUPABASE_REFRESH_TOKEN=" + strconv.Quote(credential.RefreshToken),
	}
	if credential.PublishableKey != "" {
		lines = append(lines, "PANEWIRE_SUPABASE_PUBLISHABLE_KEY="+strconv.Quote(credential.PublishableKey))
	}
	content := []byte(strings.Join(lines, "\n") + "\n")
	temporary, err := os.CreateTemp(filepath.Dir(abs), ".panewire-client-env-")
	if err != nil {
		return errors.New("create temporary credential file")
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return errors.New("set credential file mode")
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return errors.New("write credential file")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("sync credential file")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("close credential file")
	}
	if err := os.Rename(temporaryName, abs); err != nil {
		return errors.New("install credential file")
	}
	if err := os.Chmod(abs, 0600); err != nil {
		return errors.New("verify credential file mode")
	}
	return nil
}
