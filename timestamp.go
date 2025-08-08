package timestamp

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// TimeServer environment variable for NTP server.
const TimeServer = "FGRZL_TIME_SERVER"

var (
	globalClock *clock
	once        sync.Once
	initErr     error
	logger      *slog.Logger
)

// clock holds the start time for the monotonic clock.
// All fields are immutable after initialization for thread safety.
type clock struct {
	startTime int64        // Unix timestamp in milliseconds
	start     time.Time    // Monotonic reference point
	mu        sync.RWMutex // Protects against potential races during reads
}

func init() {
	// Initialize with a default logger that can be overridden
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelWarn, // Only log warnings and errors by default
	})).With("component", "timestamp")
}

// SetLogger allows users to control logging behavior.
// Pass nil to disable logging entirely.
func SetLogger(l *slog.Logger) {
	if l == nil {
		// Create a logger that discards all output
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		return
	}
	logger = l.With("component", "timestamp")
}

// DisableLogging is a convenience function to disable all logging output.
func DisableLogging() {
	SetLogger(nil)
}

// Initialize the global clock once during application startup.
func initOnce() {
	once.Do(func() {
		defer func() {
			if r := recover(); r != nil {
				// If initialization panics, fall back to system time
				logger.Error("initialization panic, falling back to system time",
					"error", r)
				globalClock = &clock{
					startTime: time.Now().UnixMilli(),
					start:     time.Now(),
				}
				initErr = fmt.Errorf("initialization panic: %v", r)
			}
		}()

		t, err := getCurrentTime()
		if err != nil {
			initErr = err
			// Even on error, we still have a valid fallback time
		}

		globalClock = &clock{
			startTime: t.UnixMilli(),
			start:     t,
		}
	})
}

// GetTimestamp returns a timestamp using monotonic elapsed time.
// This function is thread-safe and guaranteed to return monotonically increasing values.
func GetTimestamp() int64 {
	initOnce()

	if globalClock == nil {
		// Fallback if initialization somehow failed completely
		return time.Now().UnixMilli()
	}

	globalClock.mu.RLock()
	defer globalClock.mu.RUnlock()

	elapsed := time.Since(globalClock.start)
	return globalClock.startTime + elapsed.Milliseconds()
}

// GetInitializationError returns any error that occurred during initialization.
// Returns nil if initialization was successful.
func GetInitializationError() error {
	initOnce()
	return initErr
}

// GetTimeServer fetches the configured NTP server from the environment.
func GetTimeServer() string {
	return os.Getenv(TimeServer)
}

// Default list of NTP servers.
var ntpServers = []string{
	"time.google.com:123",
	"time.aws.com:123",
	"time.cloudflare.com:123",
	"time.windows.com:123",
}

// getCurrentTime attempts to fetch time from NTP or falls back to system time.
// Returns the time and any error encountered (for logging purposes).
func getCurrentTime() (time.Time, error) {
	server := GetTimeServer()

	if server == "system" {
		return time.Now(), nil
	}

	if server == "default" || server == "" {
		var lastErr error
		for _, s := range ntpServers {
			if t, err := ntpTime(s); err == nil {
				return t, nil
			} else {
				logger.Warn("NTP server failed",
					"server", s,
					"error", err)
				lastErr = err
			}
		}
		logger.Warn("All NTP servers failed, falling back to system time",
			"last_error", lastErr)
		return time.Now(), fmt.Errorf("all NTP servers failed, last error: %w", lastErr)
	}

	ntpTime, err := ntpTime(server)
	if err != nil {
		logger.Warn("Failed to fetch time from NTP server, falling back to system time",
			"server", server,
			"error", err)
		return time.Now(), fmt.Errorf("NTP server %s failed: %w", server, err)
	}

	return ntpTime, nil
}

// ntpTime fetches the current time from the specified NTP server.
// Includes proper timeout handling and validates NTP response.
func ntpTime(server string) (time.Time, error) {
	addr, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to resolve address: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	// Set a reasonable timeout for the NTP request
	deadline := time.Now().Add(5 * time.Second)
	conn.SetDeadline(deadline)

	req := make([]byte, 48)
	req[0] = 0x1B // NTP version 3, client mode

	if _, err = conn.Write(req); err != nil {
		return time.Time{}, fmt.Errorf("failed to send request: %w", err)
	}

	resp := make([]byte, 48)
	if _, err = conn.Read(resp); err != nil {
		return time.Time{}, fmt.Errorf("failed to read response: %w", err)
	}

	// Validate NTP response
	if len(resp) < 48 {
		return time.Time{}, fmt.Errorf("invalid NTP response length: %d", len(resp))
	}

	// Extract transmit timestamp (bytes 40-47)
	seconds := binary.BigEndian.Uint32(resp[40:44])
	fraction := binary.BigEndian.Uint32(resp[44:48])

	// Validate that we got a reasonable response
	if seconds == 0 {
		return time.Time{}, fmt.Errorf("invalid NTP response: zero timestamp")
	}

	ntpSeconds := float64(seconds) + float64(fraction)/0x100000000
	unixSeconds := ntpSeconds - 2208988800 // NTP epoch offset (1900-01-01 to 1970-01-01)

	ntpTime := time.Unix(int64(unixSeconds), 0)

	// Sanity check: ensure the time is reasonable (not too far in past/future)
	now := time.Now()
	if ntpTime.Before(now.Add(-24*time.Hour)) || ntpTime.After(now.Add(24*time.Hour)) {
		return time.Time{}, fmt.Errorf("NTP time %v is too far from system time %v", ntpTime, now)
	}

	return ntpTime, nil
}
