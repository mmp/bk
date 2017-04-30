#!/bin/zsh

set -e

if [[ $# -ne 1 ]]; then
    echo "usage: test_backup.sh <dir to backup>"
    exit 1
fi

: ${TMPDIR:=/tmp}

# unencrypted
SRC_DIR=$argv[1]
RESTORE_DIR=$TMPDIR/bk_restore
BK=(bk --verbose)
RDSO=rdso

export BK_DIR=$TMPDIR/bk

rm -rf $BK_DIR
mkdir $BK_DIR

$BK init
$BK backup test $SRC_DIR
sleep 1 # make sure times don't match
$BK backup test $SRC_DIR
sleep 1
$BK backup --base test test $SRC_DIR
$BK list  
$BK fsck  

$RDSO check $BK_DIR/indices/*idx
$RDSO check $BK_DIR/packs/*pack
    
for b in test `bk list | grep test@ | awk '{print $1}'`; do
    rm -rf $RESTORE_DIR
    echo Restoring backup $b
    bk restore $b $RESTORE_DIR 
    (cd $SRC_DIR; for i in **/*(^/); do cmp $i $RESTORE_DIR/$i; done)
done
rm -rf $RESTORE_DIR


#encrypted
export BK_PASSPHRASE=foobar

rm -rf $BK_DIR
mkdir $BK_DIR

$BK init --encrypt  
$BK backup test $SRC_DIR 
sleep 1
$BK backup test $SRC_DIR 
sleep 1
$BK backup --base test test $SRC_DIR
$BK list  
$BK fsck  

$RDSO check $BK_DIR/indices/*idx
$RDSO check $BK_DIR/packs/*pack

for b in test `bk list | grep test@ | awk '{print $1}'`; do
    rm -rf $RESTORE_DIR
    echo Restoring backup $b
    bk restore $b $RESTORE_DIR 
    (cd $SRC_DIR; for i in **/*(^/); do cmp $i $RESTORE_DIR/$i; done)
done

rm -rf $RESTORE_DIR
rm -rf $BK_DIR
