package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/packethost/pkg/log"
	utility "github.com/platinasystems/go-common/v2/utilities"
	avro2 "github.com/platinasystems/pcc-models/v2/bare_metal/avro"
	"github.com/tinkerbell/tink/cmd/tink-worker/publisher"
	"io"
	"math"
	"time"
)

// DockerLogCapturer is a LogCapturer that can stream docker container logs to an io.Writer.
type DockerLogCapturer struct {
	dockerClient client.ContainerAPIClient
	logger       log.Logger
	writer       io.Writer
}

// getLogger is a helper function to get logging out of a context, or use the default logger.
func (l *DockerLogCapturer) getLogger(ctx context.Context) *log.Logger {
	loggerIface := ctx.Value(loggingContextKey)
	if loggerIface == nil {
		return &l.logger
	}
	return loggerIface.(*log.Logger)
}

// NewDockerLogCapturer returns a LogCapturer that can stream container logs to a given writer.
func NewDockerLogCapturer(cli client.ContainerAPIClient, logger log.Logger, writer io.Writer) *DockerLogCapturer {
	return &DockerLogCapturer{
		dockerClient: cli,
		logger:       logger,
		writer:       writer,
	}
}

// CaptureLogs streams container logs to the capturer's writer.
func (l *DockerLogCapturer) CaptureLogs(ctx context.Context, id string) {
	l.HandleLogs(ctx, id, "", "")
}

func (l *DockerLogCapturer) HandleLogs(ctx context.Context, id string, wfId string, actionName string) {
	reader, err := l.dockerClient.ContainerLogs(ctx, id, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: false,
	})
	if err != nil {
		l.getLogger(ctx).Error(err, "failed to capture logs for container ", id)
		return
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var (
			p              pLog
			provisionerLog avro2.Log
		)
		_, _ = fmt.Fprintln(l.writer, scanner.Text())

		if err = json.Unmarshal(scanner.Bytes(), &p); err != nil {
			//_, _ = fmt.Fprintln(l.writer, fmt.Sprintf("unable to unmarshal %s: %v", scanner.Text(), err))
			// if log doesn't match the json format send it as msg
			p.Msg = scanner.Text()
		}

		if p.WorkflowID == "" && wfId != "" {
			p.WorkflowID = wfId
		}

		if p.ActionName == "" && actionName != "" {
			p.ActionName = actionName
		}

		provisionerLog.Host = publisher.Config.Host
		provisionerLog.Timestamp = parseTs(p.Ts)
		provisionerLog.Msg = p.Msg
		provisionerLog.WorkflowID = p.WorkflowID
		provisionerLog.ActionName = p.ActionName
		provisionerLog.Status = p.Status
		provisionerLog.OpStatus = p.OpStatus
		provisionerLog.Id = p.Id
		provisionerLog.Level = p.Level

		// skip empty logs
		if utility.StringIsBlank(provisionerLog.Msg) && utility.StringIsBlank(provisionerLog.Status) {
			continue
		}

		go publisher.LogQueue.Enqueue(provisionerLog)
	}
}

type pLog struct {
	avro2.Log
	Ts interface{}
}

func parseTs(ts interface{}) (timestamp int64) {
	switch t := ts.(type) {
	case float64:
		sec, dec := math.Modf(t)
		timestamp = time.Unix(int64(sec), int64(dec*(1e9))).UTC().Unix()
		return
	case string:
		if date, err := time.Parse(time.RFC3339, t); err == nil {
			timestamp = date.UTC().Unix()
			return
		}
	}
	timestamp = time.Now().UTC().Unix()
	return
}
