#!/bin/zsh

set -e

# unencrypted
export BK_DIR=/tmp/bk
rm -rf $BK_DIR
mkdir $BK_DIR
bk init
bk backup  go ~/go/src 
bk backup  go2 ~/go/src
bk backup --base go2  go3 ~/go/src
bk list  
bk fsck  

rm -rf /tmp/go 
bk restore  go /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)

rm -rf /tmp/go 
bk restore  go2 /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)

rm -rf /tmp/go 
bk restore  go3 /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)

#encrypted
export BK_PASSPHRASE=foobar
rm -rf $BK_DIR
mkdir $BK_DIR
bk init --encrypt  
bk backup  go ~/go/src 
bk backup  go2 ~/go/src 
bk backup --base go2  go3 ~/go/src
bk list  
bk fsck  

rm -rf /tmp/go 
bk restore  go /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)

rm -rf /tmp/go 
bk restore  go2 /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)

rm -rf /tmp/go 
bk restore  go3 /tmp/go 
(cd ~/go/src; for i in **/*(^/); do cmp $i /tmp/go/$i; done)
