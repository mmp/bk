// cmd/bk_e2etest/e2e.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Based on endtoendtest.go, which is Copyright(c) 2015 Google, Inc., part
// of skicka, and is licensed under the Apache License, Version 2.0.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var nDirs = 1

const BkDir = "/tmp/bk_e2e"

func main() {
	seed := os.Getpid()
	log.Printf("Seed %d", seed)
	rand.Seed(int64(seed))

	_ = os.RemoveAll(BkDir)
	_ = os.Mkdir(BkDir, 0700)
	os.Setenv("BK_DIR", BkDir)
	encrypt := randBool()
	if encrypt {
		os.Setenv("BK_PASSPHRASE", "foobar")
		runCommand("bk init --encrypt")
	} else {
		runCommand("bk init")
	}
	backupTest(randBool(), 20)
}

func randBool() bool {
	return rand.Float32() < .5
}

func expSize() int64 {
	logSize := rand.Intn(24) - 1
	s := int64(0)
	if logSize >= 0 {
		s = 1 << uint(logSize)
		s += rand.Int63n(s)
	}
	return s
}

func getCommand(c string, varargs ...string) *exec.Cmd {
	args := strings.Fields(c)
	cmd := args[0]
	args = args[1:]
	args = append(args, varargs...)
	return exec.Command(cmd, args...)
}

func runCommand(c string, args ...string) ([]byte, error) {
	log.Printf("Running %s %v", c, args)
	cmd := getCommand(c, args...)
	return cmd.Output()
}

func runButPossiblyKill(c string, args ...string) ([]byte, error) {
	log.Printf("Running %s %v", c, args)
	cmd := getCommand(c, args...)
	cmd.Stderr = os.Stderr
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	killed := false
	if (rand.Int() % 2) == 1 {
		logMs := uint(rand.Intn(16))
		wait := time.Duration(uint(1)<<logMs) * time.Millisecond
		log.Printf("Will try to kill process in %s", wait)

		time.AfterFunc(wait, func() {
			err := cmd.Process.Kill()
			if err != nil {
				log.Printf("Kill error! %v", err)
			} else {
				log.Printf("Killed process sucessfully")
				killed = true
			}
		})
	}

	err := cmd.Wait()
	if err != nil {
		log.Printf("Wait result %v", err)
	}
	if killed {
		err := filepath.Walk(BkDir,
			func(path string, info os.FileInfo, err error) error {
				if strings.HasSuffix(path, ".tmp") {
					log.Printf("Removing %s", path)
					return os.Remove(path)
				}
				return nil
			})
		if err != nil {
			log.Fatal(err)
		}
		return nil, errKilled
	}
	return out.Bytes(), err
}

var errKilled = errors.New("killed while running")

///////////////////////////////////////////////////////////////////////////

var createdFiles = make(map[string]bool)
var allNames []string

func backupTest(randomlyKill bool, iters int) {
	tmpSrc, err := ioutil.TempDir("", "bk-test-src")
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("Local src directory: %s", tmpSrc)
	defer os.RemoveAll(tmpSrc)

	tmpDst, err := ioutil.TempDir("", "bk-test-dst")
	if err != nil {
		log.Fatalf("%s", err)
	}
	log.Printf("Local dst directory: %s", tmpDst)
	defer os.RemoveAll(tmpDst)

	for i := 0; i < iters; i++ {
		// Sleep for a second before modifying files; since modtime is only
		// maintained to 1s accuracy, if we're too fast, then an
		// incremental backup may incorrectly not back up a file that was
		// actually modified.
		time.Sleep(time.Second)

		if err := update(tmpSrc); err != nil {
			log.Fatalf("%s\n", err)
		}

		// backup
		var err error
		name := fmt.Sprintf("bk-%d", i)
		if randBool() && len(allNames) > 0 {
			base := allNames[rand.Intn(len(allNames))]
			if err = backupIncremental(tmpSrc, base, name, randomlyKill); err != nil {
				log.Fatalf("%s\n", err)
			}
		} else {
			if err = backup(tmpSrc, name, randomlyKill); err != nil {
				log.Fatalf("%s\n", err)
			}
		}
		allNames = append(allNames, name)

		// restore to second tmp dir
		if err := restore(tmpDst, name); err != nil {
			log.Fatalf("%s\n", err)
		}

		if err = compare(tmpSrc, tmpDst); err != nil {
			log.Fatalf("%s", err)
		}
	}
}

func name(dir string) string {
	fodder := []string{"car", "house", "food", "cat", "monkey", "bird", "yellow",
		"blue", "fast", "sky", "table", "pen", "round", "book", "towel", "hair",
		"laugh", "airplane", "bannana", "tape", "round"}
	s := ""
	for {
		s += fodder[rand.Intn(len(fodder))]
		if _, ok := createdFiles[s]; !ok {
			break
		}
		s += "_"
	}
	createdFiles[s] = true
	return filepath.Join(dir, s)
}

func update(dir string) error {
	filesLeftToCreate := 20
	dirsLeftToCreate := 5
	log.Printf("Updating %s", dir)

	return filepath.Walk(dir,
		func(path string, stat os.FileInfo, patherr error) error {
			if patherr != nil {
				return patherr
			}

			if stat.IsDir() {
				dirsToCreate := 0
				for i := 0; i < dirsLeftToCreate; i++ {
					if rand.Intn(nDirs) == 0 {
						dirsToCreate++
						n := name(path)
						err := os.Mkdir(n, 0700)
						log.Printf("%s: created directory", n)
						if err != nil {
							return err
						}
					}
				}
				nDirs += dirsToCreate
				dirsLeftToCreate -= dirsToCreate

				filesToCreate := 0
				for i := 0; i < filesLeftToCreate; i++ {
					if rand.Intn(nDirs) == 0 {
						filesToCreate++
						n := name(path)
						f, err := os.Create(n)
						if err != nil {
							return err
						}
						newlen := expSize()
						buf := make([]byte, newlen)
						_, _ = rand.Read(buf)
						io.Copy(f, bytes.NewReader(buf))
						f.Close()
						log.Printf("%s: created file. length %d", n, newlen)
					}
				}
				filesLeftToCreate -= filesToCreate

			}

			if randBool() {
				// Advance the modified time.  Don't go into the future.
				for {
					ms := rand.Intn(10000)
					t := stat.ModTime().Add(time.Duration(ms) * time.Millisecond)
					if t.Before(time.Now()) {
						err := os.Chtimes(path, t, t)
						if err != nil {
							return err
						}
						log.Printf("%s: advanced modification time to %s", path, t.String())
						break
					}
				}
			}

			perms := stat.Mode()
			if randBool() {
				// change permissions
				newp := rand.Intn(0777)
				if stat.IsDir() {
					newp |= 0700
				} else {
					newp |= 0400
				}

				err := os.Chmod(path, os.FileMode(newp))
				if err != nil {
					return err
				}
				log.Printf("%s: changed permissions to %#o", path, newp)
				perms = os.FileMode(newp)
			}

			if randBool() && !stat.IsDir() && (perms&0600) == 0600 {
				f, err := os.OpenFile(path, os.O_WRONLY, 0666)
				if err != nil {
					return err
				}
				defer f.Close()

				// seek somewhere and write some stuff
				offset := int64(0)
				if stat.Size() > 0 {
					offset = rand.Int63n(stat.Size())
				}

				b := make([]byte, expSize())
				_, _ = rand.Read(b)
				_, err = f.WriteAt(b, offset)
				log.Printf("%s: wrote %d bytes at offset %d", path, len(b), offset)
				if err != nil {
					return err
				}

				if randBool() && stat.Size() > 0 {
					// truncate it as well
					sz := rand.Int63n(stat.Size())
					err := f.Truncate(int64(sz))
					if err != nil {
						return err
					}
					log.Printf("%s: truncated at %d", path, sz)
				}
			}

			return nil
		})
}

func backup(dir string, name string, randomlyKill bool) error {
	log.Printf("Starting backup")
	for {
		cmd := "bk backup " + name + " " + dir
		var err error
		if randomlyKill {
			_, err = runButPossiblyKill(cmd)
		} else {
			_, err = runCommand(cmd)
		}

		if err != errKilled {
			return err
		}
	}
}

func backupIncremental(dir string, base string, name string, randomlyKill bool) error {
	log.Printf("Starting incremental backup")
	for {
		cmd := "bk backup --base " + base + " " + name + " " + dir
		var err error
		if randomlyKill {
			_, err = runButPossiblyKill(cmd)
		} else {
			_, err = runCommand(cmd)
		}

		if err != errKilled {
			return err
		}
	}
}

func restore(dir string, name string) (err error) {
	log.Printf("Starting restore")

	err = os.RemoveAll(dir)
	if err != nil {
		log.Fatal(err)
	}

	cmd := "bk restore " + name + " " + dir
	_, err = runCommand(cmd)
	return err
}

func compare(patha, pathb string) error {
	mismatches := 0
	err := filepath.Walk(patha,
		func(pa string, stata os.FileInfo, patherr error) error {
			if patherr != nil {
				return patherr
			}

			// compute corresponding pathname for second file
			rest := pa[len(patha):]
			pb := filepath.Join(pathb, rest)

			statb, err := os.Stat(pb)
			if os.IsNotExist(err) {
				log.Printf("%s: not found\n", pb)
				mismatches++
				return nil
			}

			if stata.IsDir() != statb.IsDir() {
				log.Printf("%s: is file/is directory "+
					"mismatch with %s\n", pa, pb)
				mismatches++
				return nil
			}

			// compare permissions
			if stata.Mode() != statb.Mode() {
				log.Printf("%s: permissions %#o mismatch "+
					"%s permissions %#o\n", pa, stata.Mode(), pb, statb.Mode())
				mismatches++
			}

			// compare modification times
			if stata.ModTime() != statb.ModTime() {
				log.Printf("%s: mod time %s mismatches "+
					"%s mod time %s\n", pa, stata.ModTime().String(),
					pb, statb.ModTime().String())
				mismatches++
			}

			// compare sizes
			if stata.Size() != statb.Size() {
				log.Printf("%s: size %d mismatches "+
					"%s size %d\n", pa, stata.Size(), pb, statb.Size())
				mismatches++
				return nil
			}

			// compare contents
			if !stata.IsDir() {
				cmp := exec.Command("cmp", pa, pb)
				err := cmp.Run()
				if err != nil {
					log.Printf("%s and %s differ", pa, pb)
					mismatches++
				}
			}
			return nil
		})

	if err != nil {
		return err
	} else if mismatches > 0 {
		return fmt.Errorf("%d file mismatches", mismatches)
	}
	return nil
}
