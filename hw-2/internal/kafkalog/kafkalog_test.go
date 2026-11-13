package kafkalog

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestErrCode(t *testing.T) {
	cases := map[string]error{
		"NOT_ENOUGH_REPLICAS":      kerr.NotEnoughReplicas,
		"NOT_LEADER_FOR_PARTITION": fmt.Errorf("wrapped: %w", kerr.NotLeaderForPartition),
		"other":                    errors.New("dial tcp: refused"),
		"none":                     nil,
	}
	for want, err := range cases {
		if got := ErrCode(err); got != want {
			t.Errorf("ErrCode(%v) = %s, want %s", err, got, want)
		}
	}
}

func TestBatchErrorHook(t *testing.T) {
	var codes []string
	l := &Logger{
		Out:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinLevel:     kgo.LogLevelInfo,
		OnBatchError: func(c string) { codes = append(codes, c) },
	}
	l.Log(kgo.LogLevelInfo, BatchFailedMsg, "broker", "1", "err", kerr.NotEnoughReplicas, "partition", 2)
	l.Log(kgo.LogLevelInfo, "metadata update", "err", kerr.NotEnoughReplicas)
	if len(codes) != 1 || codes[0] != "NOT_ENOUGH_REPLICAS" {
		t.Fatalf("codes %v", codes)
	}
}
