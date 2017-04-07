// util/log.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package util

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

// Logger provides a simple logging system with a few different log levels;
// debugging and verbose output may both be suppressed independently.
type Logger struct {
	NErrors int
	mu      sync.Mutex
	debug   io.Writer
	verbose io.Writer
	warning io.Writer
	err     io.Writer
}

func NewLogger(verbose, debug bool) *Logger {
	l := &Logger{}
	if verbose {
		l.verbose = os.Stderr
	}
	if debug {
		l.debug = os.Stderr
	}
	l.warning = os.Stderr
	l.err = os.Stderr
	return l
}

func (l *Logger) Print(f string, args ...interface{}) {
	fmt.Printf("%s", format(f, args...))
}

func (l *Logger) Debug(f string, args ...interface{}) {
	if l == nil {
		fmt.Fprintf(os.Stderr, format(f, args...))
		return
	}

	if l.debug == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprint(l.debug, format(f, args...))
}

func (l *Logger) Verbose(f string, args ...interface{}) {
	if l == nil {
		fmt.Fprintf(os.Stderr, format(f, args...))
		return
	}

	if l.verbose == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprint(l.verbose, format(f, args...))
}

func (l *Logger) Warning(f string, args ...interface{}) {
	if l == nil {
		fmt.Fprintf(os.Stderr, format(f, args...))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprint(l.warning, format(f, args...))
}

func (l *Logger) Error(f string, args ...interface{}) {
	if l == nil {
		fmt.Fprintf(os.Stderr, format(f, args...))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.NErrors++
	fmt.Fprint(l.err, format(f, args...))
}

func (l *Logger) Fatal(f string, args ...interface{}) {
	if l == nil {
		fmt.Fprintf(os.Stderr, format(f, args...))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.NErrors++
	fmt.Fprint(l.err, format(f, args...))
	os.Exit(1)
}

// Checks the provided condition and prints a fatal error if it's false.
// The error message includes the source file and line number where the
// check failed.  An optional message specified with printf-style
// formatting may be provided to print with the error message.
func (l *Logger) Check(v bool, msg ...interface{}) {
	if v {
		return
	}

	if l != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.NErrors++
	}

	if len(msg) == 0 {
		fmt.Fprint(l.err, format("Check failed\n"))
	} else {
		f := msg[0].(string)
		fmt.Fprint(l.err, format(f, msg[1:]...))
	}
	os.Exit(1)
}

// Similar to Check, CheckError prints a fatal error if the given error is
// non-nil.  It also takes an optional format string.
func (l *Logger) CheckError(err error, msg ...interface{}) {
	if err == nil {
		return
	}

	if l != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.NErrors++
	}

	if len(msg) == 0 {
		fmt.Fprint(l.err, format("Error: %+v\n", err))
	} else {
		f := msg[0].(string)
		fmt.Fprint(l.err, format(f, msg[1:]...))
	}
	os.Exit(1)
}

func format(f string, args ...interface{}) string {
	// Two levels up the call stack
	_, fn, line, _ := runtime.Caller(2)
	// Last two components of the path
	fnline := path.Base(path.Dir(fn)) + "/" + path.Base(fn) + fmt.Sprintf(":%d", line)
	s := fmt.Sprintf("%-25s: ", fnline)
	s += fmt.Sprintf(f, args...)
	if !strings.HasSuffix(s, "\n") {
		s += "\n"
	}
	return s
}
