// storage/ratelimit.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Taken from skicka: gdrive/readers.go. (c)2015, Google, Inc. (BSD Licensed).
// Updated to use time.Ticker

package storage

import (
	"io"
	"sync"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// Bandwidth-limiting io.Reader

// Maximum number of bytes of data that we are currently allowed to upload
// or download given the bandwidth limits set by the user, if any.  These
// values are reduced by the rateLimitedReader.Read() method when data is
// uploaded or downloaded, and are periodically increased by the task
// launched by launchBandwidthTask().
var availableUploadBytes, availableDownloadBytes int
var uploadBandwidthLimited, downloadBandwidthLimited bool
var bandwidthTaskRunning bool

// Mutex to protect available{Upload,Download}Bytes.
var bandwidthMutex sync.Mutex
var bandwidthCond = sync.NewCond(&bandwidthMutex)

func InitBandwidthLimit(uploadBytesPerSecond, downloadBytesPerSecond int) {
	log.Check(!bandwidthTaskRunning)

	uploadBandwidthLimited = uploadBytesPerSecond != 0
	downloadBandwidthLimited = downloadBytesPerSecond != 0

	bandwidthMutex.Lock()
	defer bandwidthMutex.Unlock()
	bandwidthTaskRunning = true

	// 1/8th of a second
	ticker := time.NewTicker(125 * time.Millisecond)

	go func() {
		for {
			<-ticker.C

			bandwidthMutex.Lock()

			// Release 1/8th of the per-second limit every 8th of a second.
			// The 94/100 factor in the amount released adds some slop to
			// account for TCP/IP overhead and HTTP headers in an effort to
			// have the actual bandwidth used not exceed the desired limit.
			availableUploadBytes += uploadBytesPerSecond * 94 / 100 / 8
			if availableUploadBytes > uploadBytesPerSecond {
				// Don't ever queue up more than one second's worth of
				// transmission.
				availableUploadBytes = uploadBytesPerSecond
			}
			availableDownloadBytes += downloadBytesPerSecond * 94 / 100 / 8
			if availableDownloadBytes > downloadBytesPerSecond {
				availableDownloadBytes = downloadBytesPerSecond
			}

			// Wake up any threads that are waiting for more bandwidth now
			// that we've doled some more out.
			bandwidthCond.Broadcast()
			bandwidthMutex.Unlock()
		}
	}()
}

// rateLimitedReader is an io.Reader implementation that returns no
// more bytes than the current value of *availableBytes.  Thus, as long as
// the upload and download paths wrap the underlying io.Readers for local
// files and downloads from GCS (respectively), then we should stay under
// the bandwidth per second limit.
type rateLimitedReader struct {
	R              io.Reader
	availableBytes *int
}

func NewLimitedUploadReader(r io.Reader) io.Reader {
	if uploadBandwidthLimited {
		return rateLimitedReader{R: r, availableBytes: &availableUploadBytes}
	}
	return r
}

func NewLimitedDownloadReader(r io.Reader) io.Reader {
	if downloadBandwidthLimited {
		return rateLimitedReader{R: r, availableBytes: &availableDownloadBytes}
	}
	return r
}

func (lr rateLimitedReader) Read(dst []byte) (int, error) {
	// Loop until some amount of bandwidth is available.
	bandwidthMutex.Lock()
	for {
		log.Check(*lr.availableBytes >= 0)

		if *lr.availableBytes > 0 {
			break
		} else {
			// No further uploading is possible at the moment; wait for the
			// thread that periodically doles out more bandwidth to do its
			// thing, at which point it will signal the condition variable.
			bandwidthCond.Wait()
		}
	}

	// The caller would like us to return up to this many bytes...
	n := len(dst)

	// but don't do more than we're allowed to...
	if n > *lr.availableBytes {
		n = *lr.availableBytes
	}

	// Update the budget for the maximum amount of what we may consume and
	// relinquish the lock so that other workers can claim bandwidth.
	*lr.availableBytes -= n
	bandwidthMutex.Unlock()

	read, err := lr.R.Read(dst[:n])
	if read < n {
		// It may turn out that the amount we read from the original
		// io.Reader is less than the caller asked for; in this case,
		// we give back the bandwidth that we reserved but didn't use.
		bandwidthMutex.Lock()
		*lr.availableBytes += n - read
		bandwidthMutex.Unlock()
	}

	return read, err
}
