package utils

import (
	"math/rand"
	"time"
)

/*
	Retries using exponential backoff with jitter.
*/

const (
	// Retry configuration
	MaxRetryAttempts     = 5
	InitialRetryInterval = 200 * time.Millisecond
	MaxRetryInterval     = 2 * time.Second
	JitterFactor         = 0.2 // 20% jitter
)

// RetryState tracks the state of retries for a particular operation
type RetryState struct {
	Attempt     int
	LastBackoff time.Duration
	StartTime   time.Time
}

// NewRetryState creates a new retry state
func NewRetryState() *RetryState {
	return &RetryState{
		Attempt:     0,
		LastBackoff: InitialRetryInterval,
		StartTime:   time.Now(),
	}
}

// NextBackoff calculates the next backoff duration with exponential increase and jitter
func (rs *RetryState) NextBackoff() time.Duration {
	if rs.Attempt >= MaxRetryAttempts {
		return 0
	}

	// Calculate exponential backoff
	backoff := rs.LastBackoff * 2
	if backoff > MaxRetryInterval {
		backoff = MaxRetryInterval
	}

	// Add jitter
	jitter := float64(backoff) * JitterFactor
	backoff = backoff + time.Duration(rand.Float64()*jitter)

	rs.LastBackoff = backoff
	rs.Attempt++

	return backoff
}

// ShouldRetry determines if another retry attempt should be made
func (rs *RetryState) ShouldRetry() bool {
	return rs.Attempt < MaxRetryAttempts
}

// GetRetryMetrics returns metrics about the retry attempts
func (rs *RetryState) GetRetryMetrics() map[string]interface{} {
	return map[string]interface{}{
		"attempt":      rs.Attempt,
		"last_backoff": rs.LastBackoff,
		"total_time":   time.Since(rs.StartTime),
	}
}
