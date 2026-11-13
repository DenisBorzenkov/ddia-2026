// Package kafkalog пишет логи клиента franz-go в slog.
package kafkalog

import (
	"context"
	"errors"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchFailedMsg - сообщение franz-go, когда брокер вернул ошибку на батч в produce-запросе.
// Ретраибельные ошибки (NOT_ENOUGH_REPLICAS, NOT_LEADER_FOR_PARTITION, REQUEST_TIMED_OUT)
// клиент повторяет сам, наружу они видны только в логе.
const BatchFailedMsg = "batch in a produce request failed"

// Logger - kgo.Logger поверх slog. OnBatchError, если задан, вызывается с кодом ошибки
// брокера на каждый неудачный produce-батч.
type Logger struct {
	Out          *slog.Logger
	MinLevel     kgo.LogLevel
	OnBatchError func(code string)
}

var _ kgo.Logger = (*Logger)(nil)

// Level - уровень, начиная с которого franz-go присылает сообщения.
func (l *Logger) Level() kgo.LogLevel { return l.MinLevel }

// Log пишет сообщение franz-go.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if msg == BatchFailedMsg && l.OnBatchError != nil {
		l.OnBatchError(ErrCode(errFrom(keyvals)))
	}
	var lv slog.Level
	switch level {
	case kgo.LogLevelError:
		lv = slog.LevelError
	case kgo.LogLevelWarn:
		lv = slog.LevelWarn
	case kgo.LogLevelInfo:
		lv = slog.LevelInfo
	default:
		lv = slog.LevelDebug
	}
	l.Out.Log(context.Background(), lv, msg, append([]any{"component", "kgo"}, keyvals...)...)
}

func errFrom(keyvals []any) error {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if k, _ := keyvals[i].(string); k == "err" {
			err, _ := keyvals[i+1].(error)
			return err
		}
	}
	return nil
}

// ErrCode - код ошибки Kafka (NOT_ENOUGH_REPLICAS и т.п.) или "other".
func ErrCode(err error) string {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	if err == nil {
		return "none"
	}
	return "other"
}
