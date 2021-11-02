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

// Digits returns the number of digits in the decimal (base 10) representation
// of the absolute value of x.
func Digits(x int) int {
	if x < 0 {
		x = -x // take absolute value
	}
	// Since most uses of this routine will be 1-3 digits, a linear scan provides
	// the fastest performance (compared to FP math of floor(log10(x)), or lookup
	// tables and log2/CLZ estimation)
	switch {
	case x < 10:
		return 1
	case x < 100:
		return 2
	case x < 1000:
		return 3
	case x < 10000:
		return 4
	case x < 100000:
		return 5
	case x < 1000000:
		return 6
	case x < 10000000:
		return 7
	case x < 100000000:
		return 8
	case x < 1000000000:
		return 9
	case x < 10000000000:
		return 10
	case x < 100000000000:
		return 11
	case x < 1000000000000:
		return 12
	case x < 10000000000000:
		return 13
	case x < 100000000000000:
		return 14
	case x < 1000000000000000:
		return 15
	case x < 10000000000000000:
		return 16
	case x < 100000000000000000:
		return 17
	case x < 1000000000000000000:
		return 18
	}
	return 0 // unreachable
}

// DigitsSigned returns the number of digits in the decimal (base 10)
// representation of the absolute value of x (plus one if x is negative).
func DigitsSigned(x int) int {
	n := Digits(x)
	if x < 0 {
		n++
	}
	return n
}
