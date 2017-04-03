// cmd/rdso/main.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Simple tool to apply Reed-Solomon encoding to files. Provides facilities
// to check the integrity of encoded files and to recover corrupt files.

package main

import (
	"flag"
	"fmt"
	"github.com/mmp/bk/rdso"
	u "github.com/mmp/bk/util"
	"io"
	"os"
	"strings"
)

func usage() {
	fmt.Printf("usage: rdso encode [--nshards n] [--nparity n] [--hashrate r] <files...>\n")
	fmt.Printf("usage: rdso <check,restore> <files...>\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	log := u.NewLogger(true /*verbose*/, false /*debug*/)

	switch os.Args[1] {
	case "encode":
		encode(os.Args[2:])
	case "check":
		w := &countingWriter{os.Stderr, 0}
		for _, fn := range os.Args[2:] {
			err := rdso.CheckFile(fn, fn+".rs", log)
			if err != nil {
				fmt.Fprintln(os.Stderr, fn+": "+err.Error())
				os.Exit(1)
			}
		}
		os.Exit(w.Count)
	case "restore":
		for _, fn := range os.Args[2:] {
			err := rdso.RestoreFile(fn, fn+".rs", log)
			if err != nil {
				fmt.Fprintln(os.Stderr, fn+": "+err.Error())
				os.Exit(1)
			}
		}
	default:
		usage()
	}
}

type countingWriter struct {
	io.Writer
	Count int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.Count++
	return w.Writer.Write(p)
}

func encode(args []string) {
	flag := flag.NewFlagSet("encode", flag.ContinueOnError)
	nShards := flag.Int("nshards", 17, "number of data shards")
	nParity := flag.Int("nparity", 3, "number of parity shards")
	hashRate := flag.Int64("hashrate", 1024*1024, "chunk size for file hashes")
	err := flag.Parse(args)
	if err != nil {
		os.Exit(1)
	}

	for _, fn := range flag.Args() {
		if strings.HasSuffix(fn, ".rs") {
			fmt.Println(fn, ": skipping Reed-Solomon encoding of .rs file")
			continue
		}
		rsfn := fn + ".rs"
		err := rdso.EncodeFile(fn, rsfn, *nShards, *nParity, *hashRate)
		if err != nil {
			fmt.Println(fn + ": " + err.Error())
		}
		fmt.Printf("%s: created Reed-Solomon encoding file\n", rsfn)
	}
}
