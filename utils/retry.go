package utils

import (
	"math/rand"
	"time"
)

const (
	// Retry configuration
	MaxRetryAttempts     = 3
	InitialRetryInterval = 200 * time.Millisecond
	MaxTotalTimeout      = 2 * time.Second
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

	// Add jitter
	jitter := float64(backoff) * JitterFactor
	backoff = backoff + time.Duration(rand.Float64()*jitter)

	// Check if adding this backoff would exceed the total timeout
	elapsed := time.Since(rs.StartTime)
	if elapsed+backoff > MaxTotalTimeout {
		// If we would exceed the timeout, return the remaining time
		remaining := MaxTotalTimeout - elapsed
		if remaining <= 0 {
			return 0
		}
		backoff = remaining
	}

	rs.LastBackoff = backoff
	rs.Attempt++

	return backoff
}

// ShouldRetry determines if another retry attempt should be made
func (rs *RetryState) ShouldRetry() bool {
	if rs.Attempt >= MaxRetryAttempts {
		return false
	}
	// Also check if we've exceeded the total timeout
	return time.Since(rs.StartTime) < MaxTotalTimeout
}

// GetRetryMetrics returns metrics about the retry attempts
func (rs *RetryState) GetRetryMetrics() map[string]interface{} {
	return map[string]interface{}{
		"attempt":      rs.Attempt,
		"last_backoff": rs.LastBackoff,
		"total_time":   time.Since(rs.StartTime),
		"timeout":      MaxTotalTimeout,
	}
}
