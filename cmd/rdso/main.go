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

	open := func(fn string) (*os.File, *os.File) {
		r, err := os.Open(fn)
		if err != nil {
			fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
			os.Exit(1)
		}
		rsr, err := os.Open(fn + ".rs")
		if err != nil {
			fmt.Fprint(os.Stderr, fn, ".rs: ", err, "\n")
			os.Exit(1)
		}
		return r, rsr
	}

	switch os.Args[1] {
	case "encode":
		encode(os.Args[2:])
	case "check":
		for _, fn := range os.Args[2:] {
			r, rsr := open(fn)
			if err := rdso.Check(r, rsr, log); err != nil {
				fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
				os.Exit(1)
			} else {
				fmt.Print(fn, ": successfully verified integrity\n")
			}
			r.Close()
			rsr.Close()
		}
	case "restore":
		for _, fn := range os.Args[2:] {
			r, rsr := open(fn)
			info, err := r.Stat()
			if err != nil {
				fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
				os.Exit(1)
			}

			w, err := os.Create(fn + ".restored")
			if err != nil {
				fmt.Fprint(os.Stderr, fn, ".restored: ", err, "\n")
				os.Exit(1)
			}
			rsw, err := os.Create(fn + ".rs.restored")
			if err != nil {
				fmt.Fprint(os.Stderr, fn, ".rs.restored: ", err, "\n")
				os.Exit(1)
			}

			if err := rdso.Restore(r, rsr, info.Size(), w, rsw, log); err != nil {
				fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
				os.Exit(1)
			}

			r.Close()
			rsr.Close()

			if err = w.Close(); err != nil {
				fmt.Fprint(os.Stderr, fn, ".restored: ", err, "\n")
				os.Exit(1)
			}
			if err = rsw.Close(); err != nil {
				fmt.Fprint(os.Stderr, fn, ".rs.restored: ", err, "\n")
				os.Exit(1)
			}
		}
	default:
		usage()
	}
}

func encode(args []string) {
	flag := flag.NewFlagSet("encode", flag.ContinueOnError)
	nShards := flag.Int("nshards", 17, "number of data shards")
	nParity := flag.Int("nparity", 3, "number of parity shards")
	hashRate := flag.Int("hashrate", 1024*1024, "chunk size for file hashes")
	err := flag.Parse(args)
	if err != nil {
		os.Exit(1)
	}

	for _, fn := range flag.Args() {
		if strings.HasSuffix(fn, ".rs") {
			fmt.Print(fn, ": skipping Reed-Solomon encoding of .rs file\n")
			continue
		}
		r, err := os.Open(fn)
		if err != nil {
			fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
			os.Exit(1)
		}
		info, err := r.Stat()
		if err != nil {
			fmt.Fprint(os.Stderr, fn, ": ", err, "\n")
			os.Exit(1)
		}

		w, err := os.Create(fn + ".rs")
		if err != nil {
			fmt.Fprint(os.Stderr, fn, ".rs: ", err, "\n")
			os.Exit(1)
		}

		err = rdso.Encode(r, info.Size(), w, *nShards, *nParity, *hashRate)
		if err != nil {
			fmt.Print(fn, ": ", err, "\n")
		}
		fmt.Print(fn, ".rs: created Reed-Solomon encoding file\n")
	}
}
