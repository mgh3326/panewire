package core

import (
	cryptorand "crypto/rand"
	"math/big"
	"time"
)

const (
	retryBase = time.Second
	retryCap  = 5 * time.Minute
)

// FullJitterBackoff returns a delay in [0, min(1s*2^(attempt-1), 5m)]. The
// caller supplies the draw so deterministic fixtures never sleep or rely on
// wall-clock randomness.
func FullJitterBackoff(attempt int, draw func(upperExclusive int64) int64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	cap := retryBase
	for i := 1; i < attempt && cap < retryCap; i++ {
		if cap > retryCap/2 {
			cap = retryCap
			break
		}
		cap *= 2
	}
	if draw == nil {
		draw = secureJitterDraw
	}
	n := cap.Nanoseconds() + 1
	v := draw(n)
	if v < 0 {
		v = 0
	}
	if v >= n {
		v = n - 1
	}
	return time.Duration(v)
}

func secureJitterDraw(upperExclusive int64) int64 {
	if upperExclusive <= 1 {
		return 0
	}
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(upperExclusive))
	if err != nil {
		return upperExclusive / 2
	}
	return value.Int64()
}

func RetryCap(attempt int) time.Duration {
	return FullJitterBackoff(attempt, func(upper int64) int64 { return upper - 1 })
}
