package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mgh3326/panewire/sentinel"
)

type sentinelTelegramEnv struct {
	BotToken string
	ChatID   string
}

type sentinelNotifierDeps struct {
	HTTPClient            *http.Client
	BaseURL               string
	AllowInsecureForTests bool
}

// loadSentinelTelegramEnv shares the repository's strict credential-file
// guard.  It has no default location and returns no secret-bearing errors.
func loadSentinelTelegramEnv(path string) (sentinelTelegramEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return sentinelTelegramEnv{}, errors.New("sentinel Telegram env must be a regular mode-0600 file")
	}
	env := sentinelTelegramEnv{BotToken: values["TG_BOT_TOKEN"], ChatID: values["TG_CHAT_ID"]}
	if !validTelegramSecret(env.BotToken) || !validTelegramSecret(env.ChatID) {
		return sentinelTelegramEnv{}, errors.New("sentinel Telegram env is missing required values")
	}
	return env, nil
}

func validTelegramSecret(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsRune(value, '\x00')
}

type telegramNotifier struct {
	baseURL *url.URL
	token   string
	chatID  string
	client  *http.Client
}

func newTelegramNotifier(env sentinelTelegramEnv, deps sentinelNotifierDeps) (*telegramNotifier, error) {
	if !validTelegramSecret(env.BotToken) || !validTelegramSecret(env.ChatID) {
		return nil, errors.New("sentinel Telegram credentials are invalid")
	}
	base := deps.BaseURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "https" && !deps.AllowInsecureForTests) {
		return nil, errors.New("sentinel Telegram endpoint is invalid")
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return &telegramNotifier{baseURL: parsed, token: env.BotToken, chatID: env.ChatID, client: client}, nil
}

func (n *telegramNotifier) Send(ctx context.Context, alert sentinel.Alert) error {
	body, err := json.Marshal(struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}{ChatID: n.chatID, Text: sentinel.FormatAlert(alert)})
	if err != nil {
		return errors.New("sentinel Telegram message encoding failed")
	}
	requestURL := *n.baseURL
	requestURL.Path = strings.TrimSuffix(requestURL.Path, "/") + "/bot" + url.PathEscape(n.token) + "/sendMessage"
	requestContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return errors.New("sentinel Telegram request failed")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := n.client.Do(request)
	if err != nil {
		return errors.New("sentinel Telegram request failed")
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return errors.New("sentinel Telegram request rejected")
	}
	var result struct {
		OK *bool `json:"ok"`
	}
	if len(responseBody) > 0 && json.Unmarshal(responseBody, &result) == nil && result.OK != nil && !*result.OK {
		return errors.New("sentinel Telegram request rejected")
	}
	return nil
}

var _ sentinel.Notifier = (*telegramNotifier)(nil)
