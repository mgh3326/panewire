package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/handoffkeep/client"
)

// briefDocPrefix marks a -p value that names an hk document instead of a file
// (docs/config.md, "Brief from hk"). The rest of the value is the document key.
const briefDocPrefix = "hk:doc/"

// briefDocKeyMaxBytes is hk's own document key bound.
const briefDocKeyMaxBytes = 512

// briefDocKey reads the -p grammar: doc is false for a plain path, which reads
// as the file it always did. For hk:doc/KEY it returns KEY, refusing one hk
// could never hold: empty, a leading slash, a control character, "..", not
// UTF-8 or longer than hk's bound.
func briefDocKey(prompt string) (key string, doc bool, err error) {
	key, doc = strings.CutPrefix(prompt, briefDocPrefix)
	if !doc {
		return "", false, nil
	}
	if key == "" || strings.HasPrefix(key, "/") || len(key) > briefDocKeyMaxBytes || !utf8.ValidString(key) || strings.Contains(key, "..") || strings.IndexFunc(key, unicode.IsControl) >= 0 {
		return "", true, fail(exitUsage, "invalid hk brief doc key %q: -p hk:doc/KEY needs a nonempty key of at most %d bytes with no leading slash, control character or \"..\"", key, briefDocKeyMaxBytes)
	}
	return key, true, nil
}

// readBrief returns the brief -p names. A plain path is read from the file and
// its error is returned as is, so each caller keeps its own refusal for it. An
// hk:doc/KEY brief is the body of hk document KEY, read with the svc-panewire
// client before the caller does anything else; every way that read can fail
// is a one-line refusal naming the key — an unconfigured credential or a
// refused endpoint (exit 70, whose text names no value), a key hk does not
// have (2), an answer that cannot be read or an hk that cannot answer (75) —
// and a body that is empty or longer than limit (0: no limit) is refused the
// same way. A read brief prints brief_doc=KEY, the record that the brief
// lives in hk.
func (a *App) readBrief(ctx context.Context, prompt string, limit int) ([]byte, error) {
	key, doc, err := briefDocKey(prompt)
	if err != nil {
		return nil, err
	}
	if !doc {
		return os.ReadFile(expand(prompt))
	}
	if _, err := newHKClient(); err != nil {
		return nil, fail(exitConfig, "hk brief doc %s: %v", key, err)
	}
	body, err := hkBriefDocument(ctx, key)
	switch {
	case err == nil:
	case errors.Is(err, errHKNotFound):
		return nil, fail(exitUsage, "hk brief doc %s not found; put the brief in hk under that key or pass -p with a file", key)
	case errors.Is(err, errHKUnreadable):
		return nil, fail(exitStateConflict, "hk brief doc %s: unreadable hk answer", key)
	default:
		return nil, fail(exitStateConflict, "hk brief doc %s unavailable: %s", key, hkErrText(err))
	}
	if strings.TrimSpace(body) == "" {
		return nil, fail(exitUsage, "hk brief doc %s is empty", key)
	}
	if limit > 0 && len(body) > limit {
		return nil, fail(exitUsage, "hk brief doc %s is %d bytes; pass -p with a nonempty UTF-8 brief of at most %d KiB", key, len(body), limit>>10)
	}
	fmt.Fprintf(a.Out, "brief_doc=%s source=hk bytes=%d\n", key, len(body))
	return []byte(body), nil
}

// hkBriefDocument is the body of hk document key. A key hk does not have is
// errHKNotFound; a 2xx answer that is not a document for key — not JSON, or
// another key — is errHKUnreadable, as the other hk reads treat an answer
// they cannot read (hkTaskCall). A document stored with no body reads as "".
func hkBriefDocument(ctx context.Context, key string) (string, error) {
	raw, answered, err := hkAnswer(func(c client.Client) error {
		_, found, e := c.GetDocument(ctx, key)
		if e == nil && !found {
			e = errHKNotFound
		}
		return e
	})
	if !answered {
		return "", err
	}
	var doc struct {
		Key  string `json:"key"`
		Body string `json:"body"`
	}
	if json.Unmarshal(raw, &doc) != nil || doc.Key != key {
		return "", errHKUnreadable
	}
	return doc.Body, nil
}
