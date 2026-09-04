package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/coder/websocket"
)

func TestR19ListenListsAcceptLoopbackAndTailnet(t *testing.T) {
	addresses, err := hubListenAddresses([]string{"--hub-auth", "ignored", "--listen", "127.0.0.1:9377,100.64.0.1:9377"})
	if err != nil || len(addresses) != 2 || addresses[1] != "100.64.0.1:9377" {
		t.Fatalf("addresses=%v err=%v", addresses, err)
	}
	if _, err := hubListenAddress("192.0.2.1:9377"); err == nil {
		t.Fatal("non-tailnet public bind accepted")
	}
}

func TestR19HubURLFallbackAndPreference(t *testing.T) {
	attempts := make([]string, 0, 3)
	dial := func(_ context.Context, endpoint string, _ *websocket.DialOptions) (*websocket.Conn, *http.Response, error) {
		attempts = append(attempts, endpoint)
		if len(attempts) == 1 {
			return nil, nil, errors.New("down")
		}
		return nil, nil, nil
	}
	client, err := NewHubClient(HubClientConfig{URLs: []string{"ws://100.64.0.1:9377", "ws://127.0.0.1:9377"}, MachineID: "node-a", Token: "fixture", AllowInsecureForTests: true, Dial: dial})
	if err != nil {
		t.Fatal(err)
	}
	_, endpoint, err := client.dialAny(context.Background())
	if err != nil || endpoint != client.endpoints[1] {
		t.Fatalf("fallback endpoint=%q err=%v", endpoint, err)
	}
	client.endpoint = client.endpoints[1]
	_, endpoint, switched := client.dialPreferred(context.Background())
	if !switched || endpoint != client.endpoints[0] {
		t.Fatalf("preference endpoint=%q switched=%v", endpoint, switched)
	}
}

func TestR19UpdateChecksumFailureDoesNotReplace(t *testing.T) {
	asset := []byte("new binary")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(asset) }))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "panewire")
	if err := os.WriteFile(path, []byte("old binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := applyHubUpdate(context.Background(), server.Client(), path, server.URL, "0000000000000000000000000000000000000000000000000000000000000000"); err == nil {
		t.Fatal("checksum mismatch accepted")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != "old binary" {
		t.Fatalf("binary changed: %q err=%v", got, err)
	}
	hash := sha256.Sum256(asset)
	if err := applyHubUpdate(context.Background(), server.Client(), path, server.URL, hex.EncodeToString(hash[:])); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(path)
	if string(got) != string(asset) {
		t.Fatalf("got %q", got)
	}
}
