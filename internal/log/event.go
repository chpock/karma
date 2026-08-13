package log

import (
	"context"
	"log/slog"
)

// Event writes one structured runtime event with the mandatory component and
// stable event name fields.
func Event(ctx context.Context, level slog.Level, component, event, message string, attrs ...slog.Attr) {
	base := make([]slog.Attr, 0, len(attrs)+2)
	base = append(base, slog.String("component", component), slog.String("event", event))
	base = append(base, attrs...)
	slog.LogAttrs(ctx, level, message, base...)
}

func Enabled(ctx context.Context, level slog.Level) bool {
	return slog.Default().Enabled(ctx, level)
}

func Debug(component, event, message string, attrs ...slog.Attr) {
	Event(context.Background(), slog.LevelDebug, component, event, message, attrs...)
}

func DebugContext(ctx context.Context, component, event, message string, attrs ...slog.Attr) {
	Event(ctx, slog.LevelDebug, component, event, message, attrs...)
}

func Info(component, event, message string, attrs ...slog.Attr) {
	Event(context.Background(), slog.LevelInfo, component, event, message, attrs...)
}

func InfoContext(ctx context.Context, component, event, message string, attrs ...slog.Attr) {
	Event(ctx, slog.LevelInfo, component, event, message, attrs...)
}

func Warn(component, event, message string, attrs ...slog.Attr) {
	Event(context.Background(), slog.LevelWarn, component, event, message, attrs...)
}

func WarnContext(ctx context.Context, component, event, message string, attrs ...slog.Attr) {
	Event(ctx, slog.LevelWarn, component, event, message, attrs...)
}

func Error(component, event, message string, attrs ...slog.Attr) {
	Event(context.Background(), slog.LevelError, component, event, message, attrs...)
}

func ErrorContext(ctx context.Context, component, event, message string, attrs ...slog.Attr) {
	Event(ctx, slog.LevelError, component, event, message, attrs...)
}
