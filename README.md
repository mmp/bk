
# Overview

*bk* is a tool for backing things up--both raw data streams and directory
hierarchies. I wrote it because I wanted to have personal responsibility
for my data's integrity, up to and including being responsible for data
loss due to bugs in the backup system. You should probably use something
else to back up your data--[bup](https://github.com/bup/bup) is a great
choice.

That said, thanks to Google for letting me open source it.

# Features

My goal was to implement the absolute minimum number of features necessary
for my needs; the idea was that a minimal feature set (and in turn, a
minimal number of lines of code) would reduce the probability of bugs (and
in turn, the probability of data corruption).

* Data de-duplication (using a [rolling
  hash](https://en.wikipedia.org/wiki/Rolling_hash))
* Compression (gzip)
* Optional encryption (using Go's [AES](https://golang.org/pkg/crypto/aes/)
  implementation).
* Data integrity (and corruption recovery) using Reed-Solomon encoding.
* Direct backups to cloud storage.
* Ability to access backups via FUSE.

# Usage

To set up a backup repository, run:
```
% bk init /mnt/backups
```
It's assumed that the target directory exists but is empty. To backup to
Google Cloud Storage:
```
% bk init gs://somebucketname
```

For an encrypted repository,
```
% env BK_PASSPHRASE=yolo bk init --encrypt /mnt/backups
```
Though don't do it like that, since you don't want your passphrase in your
shell command history.

To back up a directory hierarchy (e.g., your home directory):
```
% bk backup /mnt/backups home ~
```
(BK_PASSPHRASE must be set if the repository is encrypted.)
Here, the backup is named "home". *bk* adds the current date and time to
the name of the backup; all available backups can be listed with "bk list".

Incremental backups are also possible:
```
% bk backup --base home-20170403-072015 /mnt/backups home ~
```

To restore from a backup:
```
% bk restore /mnt/backups home-20170403-072015 /tmp/restored
```

Run "bk help" for more information and additional commands.

# Influences

* [Venti: A New Approach to Archival
  Storage](https://www.usenix.org/legacy/events/fast02/quinlan/quinlan_html/),
  Sean Quinlan and Sean Dorward.  Hash-based archival storage, from the
  Plan 9 project.
* [A Low-bandwidth Network File
  System](https://pdos.csail.mit.edu/archive/lbfs/), Athicha
  Muthitacharoen, Benjie Chen, and David Mazieres: rolling hashes to break
  up bitstreams.
* [bup](https://github.com/bup/bup): rolling hashes, hash-based archival
  storage, all wrapped up in git packfiles. *bk*'s rolling hash code comes
  from bup.
* [Foundation](https://swtch.com/~rsc/papers/fndn-usenix2008.pdf):
  hash-based archival storage, revisiting some of Venti's design decisions,
  showed that rolling hashes (versus block-based archiving) weren't a big
  win.

In general, bup and foundation both go through some effort to provide
efficient access to hash-addressed data without loading an entire index
that goes from hashes to storage locations into memory.  For my use of
*bk*, the indices are a few hundred MB, so they're just all loaded at
startup time.  Note that this isn't an ideal approach when using cloud
storage; something along the lines of Foundation's approach (or keeping a
local cache of the index) would probably be better.

# FAQs that no one has asked

Q: Wouldn't it be easier to just buy a Time Capsule?

A: Enjoy your "[sparse bundle in
use](https://discussions.apple.com/thread/4282352?start=0&tstart=0)" errors
that leave all of your backups corrupt and irrecoverable but aren't
reported until you try to restore.

Q: Isn't most of this functionality provided by
[upspin](https://upspin.io)?

A: It looks like it, especially as they implement the rest of the
infrastructure for some of their key [use
cases](https://github.com/upspin/upspin/issues/19).

Q: Why did you invent your own packfile format rather than using git's?

A: *bk*'s pack files are simpler than git's (but don't have many of their
advantages, like efficient lookups after just few seeks in index files,
without needing to read them all into memory.) OTOH, *bk* uses
[SHAKE256](https://en.wikipedia.org/wiki/SHA-3) to hash data blobs into 32
bytes of hash. git's choice of SHA-1 now looks [somewhat
unfortunate](https://security.googleblog.com/2017/02/announcing-first-sha1-collision.html),
though for personal backups, this probably isn't something to worry much
about.

Q: Why not use bup?

A: You should use bup. It has lots of users, which makes it less likely to
have subtle bugs.  I wrote *bk* for fun (Go is fun) and because I wanted to
own responsibility for my bits. Also, bup doesn't directly support
encryption or uploading directly to
[GCS](https://cloud.google.com/storage/).

Q: Your use of Check() and CheckError() isn't idiomatic Go error handling.

A: That's not a question. For a backup system, I believe that most errors
should cause the system to immediately stop and fail obviously rather than
making an attempt to recover (since the recovery code paths won't be well
exercised and are thus likely to be buggy). Given this decision, I'd rather
have those checks take a single line of code rather than three lines to
test the error against nil and then panic.
