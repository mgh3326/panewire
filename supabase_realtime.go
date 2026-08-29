package panewire

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// realtimeHintProbe is a small, dependency-free Phoenix Broadcast client.  It
// is intentionally best-effort: the smoke's durable proof is the RPC claim
// poll, while this probe records a message-id hint when private Realtime is
// configured and available.
type realtimeHintProbe struct {
	mu       sync.Mutex
	receiver *realtimeSocket
	observed map[string]bool
}

func newRealtimeHintProbe() smokeHintProbe {
	return &realtimeHintProbe{observed: map[string]bool{}}
}

func (p *realtimeHintProbe) Start(ctx context.Context, receiver smokeClientEnv, channel string) error {
	if receiver.PublishableKey == "" {
		return errors.New("publishable key missing")
	}
	socket, err := connectRealtime(ctx, receiver, channel)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.receiver = socket
	p.mu.Unlock()
	go p.readHints(socket)
	return nil
}

func (p *realtimeHintProbe) Broadcast(ctx context.Context, sender smokeClientEnv, channel, messageID string) error {
	if sender.PublishableKey == "" {
		return errors.New("publishable key missing")
	}
	socket, err := connectRealtime(ctx, sender, channel)
	if err != nil {
		return err
	}
	defer socket.Close()
	return socket.sendPhoenix("2", "2", channel, "broadcast", map[string]any{
		"type": "broadcast", "event": "panewire_hint",
		"payload": map[string]string{"message_id": messageID},
	})
}

func (p *realtimeHintProbe) Observed(_ context.Context, messageID string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.observed[messageID], nil
}

func (p *realtimeHintProbe) Close() error {
	p.mu.Lock()
	socket := p.receiver
	p.receiver = nil
	p.mu.Unlock()
	if socket == nil {
		return nil
	}
	return socket.Close()
}

func (p *realtimeHintProbe) readHints(socket *realtimeSocket) {
	for {
		payload, err := socket.readText()
		if err != nil {
			return
		}
		var message any
		if json.Unmarshal(payload, &message) != nil {
			continue
		}
		for _, messageID := range realtimeMessageIDs(message) {
			p.mu.Lock()
			p.observed[messageID] = true
			p.mu.Unlock()
		}
	}
}

func realtimeMessageIDs(value any) []string {
	var result []string
	var visit func(any)
	visit = func(v any) {
		switch typed := v.(type) {
		case map[string]any:
			if id, ok := typed["message_id"].(string); ok && id != "" {
				result = append(result, id)
			}
			for _, child := range typed {
				visit(child)
			}
		case []any:
			for _, child := range typed {
				visit(child)
			}
		}
	}
	visit(value)
	return result
}

// unavailableHintProbe is used only when a test deliberately declines to
// provide a Realtime fixture.  Polling remains the authoritative correction.
type unavailableHintProbe struct{}

func (unavailableHintProbe) Start(context.Context, smokeClientEnv, string) error {
	return errors.New("unavailable")
}
func (unavailableHintProbe) Broadcast(context.Context, smokeClientEnv, string, string) error {
	return errors.New("unavailable")
}
func (unavailableHintProbe) Observed(context.Context, string) (bool, error) {
	return false, errors.New("unavailable")
}
func (unavailableHintProbe) Close() error { return nil }

func urlForSmoke(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid Supabase URL")
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

type realtimeSocket struct {
	conn net.Conn
	read *bufio.Reader
	mu   sync.Mutex
}

func connectRealtime(ctx context.Context, env smokeClientEnv, channel string) (*realtimeSocket, error) {
	u, err := urlForSmoke(env.URL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/realtime/v1/websocket"
	query := u.Query()
	query.Set("apikey", env.PublishableKey)
	query.Set("vsn", "1.0.0")
	u.RawQuery = query.Encode()

	keyBytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, keyBytes); err != nil {
		return nil, errors.New("prepare websocket handshake")
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	dialAddress := u.Host
	if u.Port() == "" {
		defaultPort := "80"
		if u.Scheme == "wss" {
			defaultPort = "443"
		}
		dialAddress = net.JoinHostPort(u.Hostname(), defaultPort)
	}
	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp", dialAddress)
	if err != nil {
		return nil, errors.New("connect realtime")
	}
	if u.Scheme == "wss" {
		secure := tls.Client(connection, &tls.Config{ServerName: u.Hostname(), MinVersion: tls.VersionTLS12})
		if err := secure.HandshakeContext(ctx); err != nil {
			_ = connection.Close()
			return nil, errors.New("connect realtime")
		}
		connection = secure
	}
	if strings.ContainsAny(env.AccessToken, "\r\n") || strings.ContainsAny(env.PublishableKey, "\r\n") {
		_ = connection.Close()
		return nil, errors.New("invalid realtime credentials")
	}
	requestURI := u.RequestURI()
	if _, err := fmt.Fprintf(connection, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer %s\r\napikey: %s\r\n\r\n", requestURI, u.Host, key, env.AccessToken, env.PublishableKey); err != nil {
		_ = connection.Close()
		return nil, errors.New("start realtime handshake")
	}
	reader := bufio.NewReader(connection)
	request, _ := http.NewRequest(http.MethodGet, u.String(), nil)
	response, err := http.ReadResponse(reader, request)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols || !websocketAccepts(response.Header.Get("Sec-WebSocket-Accept"), key) {
		_ = connection.Close()
		return nil, errors.New("realtime handshake rejected")
	}
	socket := &realtimeSocket{conn: connection, read: reader}
	if err := socket.join(ctx, channel, env.AccessToken); err != nil {
		_ = socket.Close()
		return nil, err
	}
	return socket, nil
}

func websocketAccepts(got, key string) bool {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return got == base64.StdEncoding.EncodeToString(sum[:])
}

func (s *realtimeSocket) join(ctx context.Context, channel, accessToken string) error {
	if err := s.sendPhoenix("1", "1", channel, "phx_join", map[string]any{
		"config": map[string]any{
			"broadcast": map[string]any{"ack": false, "self": false},
			"private":   true,
		},
		"access_token": accessToken,
	}); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err := s.conn.SetReadDeadline(deadline); err != nil {
		return errors.New("wait realtime join")
	}
	defer s.conn.SetReadDeadline(time.Time{})
	for {
		payload, err := s.readText()
		if err != nil {
			return errors.New("realtime join failed")
		}
		var message []json.RawMessage
		if json.Unmarshal(payload, &message) != nil || len(message) != 5 {
			continue
		}
		var event string
		_ = json.Unmarshal(message[3], &event)
		if event != "phx_reply" {
			continue
		}
		var reply struct {
			Status string `json:"status"`
		}
		if json.Unmarshal(message[4], &reply) != nil || reply.Status != "ok" {
			return errors.New("realtime join rejected")
		}
		return nil
	}
}

func (s *realtimeSocket) sendPhoenix(joinRef, ref, channel, event string, payload any) error {
	encoded, err := json.Marshal([]any{joinRef, ref, channel, event, payload})
	if err != nil {
		return errors.New("encode realtime message")
	}
	return s.writeFrame(0x1, encoded)
}

func (s *realtimeSocket) writeFrame(opcode byte, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(payload) > 1<<20 {
		return errors.New("realtime message too large")
	}
	var header []byte
	header = append(header, 0x80|opcode)
	switch {
	case len(payload) <= 125:
		header = append(header, 0x80|byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	default:
		header = append(header, 0x80|127)
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
		header = append(header, length[:]...)
	}
	mask := make([]byte, 4)
	if _, err := io.ReadFull(rand.Reader, mask); err != nil {
		return errors.New("mask realtime frame")
	}
	masked := append([]byte(nil), payload...)
	for i := range masked {
		masked[i] ^= mask[i%len(mask)]
	}
	if _, err := s.conn.Write(append(append(header, mask...), masked...)); err != nil {
		return errors.New("write realtime frame")
	}
	return nil
}

func (s *realtimeSocket) readText() ([]byte, error) {
	for {
		first, err := s.read.ReadByte()
		if err != nil {
			return nil, err
		}
		second, err := s.read.ReadByte()
		if err != nil {
			return nil, err
		}
		opcode := first & 0x0f
		masked := second&0x80 != 0
		length := uint64(second & 0x7f)
		if length == 126 {
			var encoded [2]byte
			if _, err := io.ReadFull(s.read, encoded[:]); err != nil {
				return nil, err
			}
			length = uint64(binary.BigEndian.Uint16(encoded[:]))
		} else if length == 127 {
			var encoded [8]byte
			if _, err := io.ReadFull(s.read, encoded[:]); err != nil {
				return nil, err
			}
			length = binary.BigEndian.Uint64(encoded[:])
		}
		if length > 1<<20 {
			return nil, errors.New("realtime frame too large")
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(s.read, mask[:]); err != nil {
				return nil, err
			}
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(s.read, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%len(mask)]
			}
		}
		switch opcode {
		case 0x1:
			return payload, nil
		case 0x8:
			return nil, io.EOF
		case 0x9:
			if err := s.writeFrame(0xA, payload); err != nil {
				return nil, err
			}
		}
	}
}

func (s *realtimeSocket) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}
