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
)

type hubTelegramEnv struct {
	BotToken string
	ChatID   string
}

type hubNotifierDeps struct {
	HTTPClient            *http.Client
	BaseURL               string
	AllowInsecureForTests bool
}

// loadHubTelegramEnv requires an explicit mode-0600 file and never includes a
// credential value in an error.
func loadHubTelegramEnv(path string) (hubTelegramEnv, error) {
	values, err := loadMode0600Env(path)
	if err != nil {
		return hubTelegramEnv{}, errors.New("hub Telegram env must be a regular mode-0600 file")
	}
	env := hubTelegramEnv{BotToken: values["TG_BOT_TOKEN"], ChatID: values["TG_CHAT_ID"]}
	if !validTelegramSecret(env.BotToken) || !validTelegramSecret(env.ChatID) {
		return hubTelegramEnv{}, errors.New("hub Telegram env is missing required values")
	}
	return env, nil
}

func validTelegramSecret(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsRune(value, '\x00')
}

type hubTelegramNotifier struct {
	baseURL *url.URL
	token   string
	chatID  string
	client  *http.Client
}

func newHubTelegramNotifier(env hubTelegramEnv, deps hubNotifierDeps) (*hubTelegramNotifier, error) {
	if !validTelegramSecret(env.BotToken) || !validTelegramSecret(env.ChatID) {
		return nil, errors.New("hub Telegram credentials are invalid")
	}
	base := deps.BaseURL
	if base == "" {
		base = "https://api.telegram.org"
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "https" && !deps.AllowInsecureForTests) {
		return nil, errors.New("hub Telegram endpoint is invalid")
	}
	client := deps.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return &hubTelegramNotifier{baseURL: parsed, token: env.BotToken, chatID: env.ChatID, client: client}, nil
}

func (notifier *hubTelegramNotifier) Send(ctx context.Context, alert HubAlert) error {
	body, err := json.Marshal(struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}{ChatID: notifier.chatID, Text: formatHubAlert(alert)})
	if err != nil {
		return errors.New("hub Telegram message encoding failed")
	}
	requestURL := *notifier.baseURL
	requestURL.Path = strings.TrimSuffix(requestURL.Path, "/") + "/bot" + url.PathEscape(notifier.token) + "/sendMessage"
	requestContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return errors.New("hub Telegram request failed")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := notifier.client.Do(request)
	if err != nil {
		return errors.New("hub Telegram request failed")
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return errors.New("hub Telegram request rejected")
	}
	var result struct {
		OK *bool `json:"ok"`
	}
	if len(responseBody) > 0 && json.Unmarshal(responseBody, &result) == nil && result.OK != nil && !*result.OK {
		return errors.New("hub Telegram request rejected")
	}
	return nil
}

var _ HubNotifier = (*hubTelegramNotifier)(nil)
