package log

import (
	"fmt"
	"io"
	"os"
)

const (
	Info = iota
	Warn
	Error
)

var Output io.Writer = os.Stdout

func Msg(level int, prompt string, format string, args ...interface{}) {
	var prefix string
	var output io.Writer
	switch level {
	case Info:
		output = Output
		prefix = "[ ]"
	case Warn:
		output = Output
		prefix = "[*]"
	case Error:
		output = os.Stderr
		prefix = "[!]"
	}
	fmt.Fprintf(output, fmt.Sprintf("%s %10s: %s\n", prefix, prompt, format), args...)
}
