package worker

import (
	"context"

	"github.com/tinkerbell/tink/cmd/tink-worker/worker"
)

type emptyLogger struct{}

func (l *emptyLogger) CaptureLogs(context.Context, string) {}

func (l *emptyLogger) HandleLogs(context.Context, string, string, string) {}

// NewEmptyLogCapturer returns an no-op log capturer.
func NewEmptyLogCapturer() worker.LogCapturer {
	return &emptyLogger{}
}
