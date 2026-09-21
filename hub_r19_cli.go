package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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
	cfEnv := flags.String("hub-cf-env", "", "optional mode-0600 CF_ACCESS_CLIENT_ID/CF_ACCESS_CLIENT_SECRET env file")
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
	var cfAccess hubCFAccessEnv
	if *cfEnv != "" {
		cfAccess, err = loadHubCFAccessEnv(*cfEnv)
		if err != nil {
			fmt.Fprintln(stderr, "update rejected: invalid Cloudflare Access env")
			return ExitConditionInvalid
		}
	}
	client, err := newHubOperatorClient(*hubURL, env.Token, cfAccess, deps, 15*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "update rejected: invalid hub URL")
		return ExitConditionInvalid
	}
	request := struct {
		Version  string   `json:"version"`
		SHA256   string   `json:"sha256"`
		URL      string   `json:"url"`
		Machines []string `json:"machines"`
	}{*version, *sha, *assetURL, strings.Split(*machines, ",")}
	body, _ := json.Marshal(request)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	response, err := client.do(ctx, http.MethodPost, "/v1/update", nil, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintln(stderr, "update unavailable")
		return ExitInternal
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		fmt.Fprintln(stderr, "update rejected")
		return ExitConditionInvalid
	}
	_, _ = io.Copy(stdout, io.LimitReader(response.Body, 64<<10))
	return ExitOK
}
