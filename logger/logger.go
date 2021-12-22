package logger

import (
	"fmt"
	"strings"
)

type Logger interface {
	Warning(f interface{}, v ...interface{})
	Debug(f interface{}, v ...interface{})
	Error(f interface{}, v ...interface{})
	Info(f interface{}, v ...interface{})
	Critical(f interface{}, v ...interface{})
}

func NewNonLogger() Logger {
	return &NonLogger{}
}

type NonLogger struct {
}

func (l *NonLogger) Warning(f interface{}, v ...interface{}) {
	fmt.Println(formatLog(f, v...))
}

func (l *NonLogger) Debug(f interface{}, v ...interface{}) {
	fmt.Println(formatLog(f, v...))
}

func (l *NonLogger) Error(f interface{}, v ...interface{}) {
	fmt.Println(formatLog(f, v...))
}

func (l *NonLogger) Info(f interface{}, v ...interface{}) {
	fmt.Println(formatLog(f, v...))
}

func (l *NonLogger) Critical(f interface{}, v ...interface{}) {
	fmt.Println(formatLog(f, v...))
}

func formatLog(f interface{}, v ...interface{}) string {
	var msg string
	switch f.(type) {
	case string:
		msg = f.(string)
		if len(v) == 0 {
			return msg
		}
		if !strings.Contains(msg, "%") {
			// do not contain format char
			msg += strings.Repeat(" %v", len(v))
		}
	default:
		msg = fmt.Sprint(f)
		if len(v) == 0 {
			return msg
		}
		msg += strings.Repeat(" %v", len(v))
	}
	return fmt.Sprintf(msg, v...)
}
