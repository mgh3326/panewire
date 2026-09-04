package panewire

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// runUpdateCLI is deliberately a narrow publisher: it can only issue the
// fixed update.available instruction; it does not gain a general remote exec.
func runUpdateCLI(args []string, stdout, stderr io.Writer, deps hubCLIDeps) int {
	if len(args) == 0 || args[0] != "publish" {
		return ExitUsage
	}
	flags := flag.NewFlagSet("panewire update publish", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	hubURL := flags.String("hub-url", "", "HTTPS hub URL")
	tokenEnv := flags.String("hub-token-env", "", "operator token env")
	version := flags.String("version", "", "release version")
	sha := flags.String("sha256", "", "asset SHA-256")
	assetURL := flags.String("url", "", "HTTPS release asset URL")
	machines := flags.String("machines", "", "comma-separated target machine IDs")
	if flags.Parse(args[1:]) != nil || flags.NArg() != 0 || *hubURL == "" || *tokenEnv == "" || *version == "" || *sha == "" || *assetURL == "" || *machines == "" {
		return ExitUsage
	}
	env, err := loadHubTokenEnv(*tokenEnv)
	if err != nil || env.MachineID != hubOperatorMachineID {
		fmt.Fprintln(stderr, "update rejected: invalid operator token env")
		return ExitConditionInvalid
	}
	endpoint, err := hubHTTPSEndpoint(*hubURL, "/v1/update", deps.AllowInsecureForTests)
	if err != nil {
		return ExitConditionInvalid
	}
	request := struct {
		Version  string   `json:"version"`
		SHA256   string   `json:"sha256"`
		URL      string   `json:"url"`
		Machines []string `json:"machines"`
	}{*version, *sha, *assetURL, strings.Split(*machines, ",")}
	body, _ := json.Marshal(request)
	httpRequest, _ := http.NewRequest(http.MethodPost, endpoint.String(), bytes.NewReader(body))
	httpRequest.Header.Set(hubAuthorizationHeader, "Bearer "+env.Token)
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		fmt.Fprintln(stderr, "update unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		fmt.Fprintln(stderr, "update rejected")
		return ExitConditionInvalid
	}
	_, _ = io.Copy(stdout, response.Body)
	return ExitOK
}
