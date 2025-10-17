#!/bin/bash
set -e


FILE1="bigfile.bin"
DIR1="dir1"

cleanup() {
  ## Clean up
  rm -f ${FILE1}
  rm -f ${FILE1}_result*
  rm -rf ${DIR1}
}

cleanup

pushd ..
cargo fmt
cargo build
popd

# Create test file.
dd if=/dev/urandom of=$FILE1 bs=100M count=1
split -b 1M --numeric-suffixes=0 --suffix-length=5 $FILE1 ${FILE1}_result.FRAG-

# Create test file in child dir.
mkdir $DIR1
pushd $DIR1
cp ../${FILE1}* .
popd


## Reconstruct
../target/debug/mtreconstruct --log debug

## Checksum
sha1sum ${FILE1}*
sha1sum ${DIR1}/*


cleanup
