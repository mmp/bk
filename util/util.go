// util/util.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package util

import (
	"fmt"
	"io"
	"log"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// ReportingReader

// Small wrapper around io.Reader that implements io.ReadCloser.
// Periodically logs how many bytes have been read and the rate of
// processing them in bytes / second.
type ReportingReader struct {
	R                        io.Reader
	Msg                      string
	start                    time.Time
	reportCounter, readBytes int64
}

const reportFrequency = 128 * 1024 * 1024

func (r *ReportingReader) Read(buf []byte) (int, error) {
	if r.start.IsZero() {
		r.start = time.Now()
		r.reportCounter = reportFrequency
		r.readBytes = 0
	}

	n, err := r.R.Read(buf)

	r.readBytes += int64(n)
	r.reportCounter -= int64(n)
	if r.reportCounter < 0 {
		r.report("")
		r.reportCounter += reportFrequency
	}

	return n, err
}

func (r *ReportingReader) report(prefix string) {
	delta := time.Since(r.start)
	bytesPerSec := int64(float64(r.readBytes) / delta.Seconds())
	log.Printf("%s%s %s [%s/s]", prefix, r.Msg, FmtBytes(r.readBytes),
		FmtBytes(bytesPerSec))
}

func (r *ReportingReader) Close() error {
	r.report("Finished. ")

	if rc, ok := r.R.(io.ReadCloser); ok {
		return rc.Close()
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////
// Utility Functions

func FmtBytes(n int64) string {
	if n >= 1024*1024*1024*1024 {
		return fmt.Sprintf("%.2f TiB", float64(n)/(1024.*1024.*
			1024.*1024.))
	} else if n >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GiB", float64(n)/(1024.*1024.*
			1024.))
	} else if n > 1024*1024 {
		return fmt.Sprintf("%.2f MiB", float64(n)/(1024.*1024.))
	} else if n > 1024 {
		return fmt.Sprintf("%.2f kiB", float64(n)/1024.)
	} else {
		return fmt.Sprintf("%d B", n)
	}
}
