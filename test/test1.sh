#!/bin/bash
set -e


FILE1="bigfile.bin"
DIR1="dir1"
COMMON_RECONSTRUCT_OPTS="-v -b 10"

cleanup() {
  ## Clean up
  rm -f ${FILE1}
  rm -f ${FILE1}_result*
  rm -rf ${DIR1}
}

checksum() {
  CHECKSUM1=$(sha1sum ${FILE1} | awk '{print $1}')
  CHECKSUM2=$(sha1sum ${FILE1}_result | awk '{print $1}')
  CHECKSUM3=$(sha1sum ${DIR1}/${FILE1} | awk '{print $1}')
  CHECKSUM4=$(sha1sum ${DIR1}/${FILE1}_result | awk '{print $1}')
  if [ $CHECKSUM1 = $CHECKSUM2 ]; then 
    echo "${FILE1} checksum ok."
  else
    echo "${FILE1} checksum bad."
    exit 1
  fi
  if [ $CHECKSUM3 = $CHECKSUM4 ]; then 
    echo "${DIR1}/${FILE1} checksum ok."
  else
    echo "${DIR1}/${FILE1} checksum bad."
    exit 1
  fi
}

prepare_test_file() {
  dd if=/dev/urandom of=$FILE1 bs=101M count=1 2> /dev/null > /dev/null
  split -b 1M --numeric-suffixes=0 --suffix-length=5 $FILE1 ${FILE1}_result.FRAG- 2> /dev/null > /dev/null
  mkdir $DIR1
  pushd $DIR1
  cp ../${FILE1}* .
  popd
}

cleanup

##### Build mtreconstruct.
pushd ..
cargo fmt
cargo build --release
popd
##### 


##### Test for --help
time ../target/release/mtreconstruct $COMMON_RECONSTRUCT_OPTS --help
echo Test "--help" Ok.
#####


##### Test for cat (std::io::copy())
prepare_test_file
time ../target/release/mtreconstruct $COMMON_RECONSTRUCT_OPTS
checksum
cleanup
echo Test "Reconstruction default" Ok.
#####


##### Test for cat (tokio::io::copy())
prepare_test_file
time ../target/release/mtreconstruct $COMMON_RECONSTRUCT_OPTS --async
checksum
cleanup
echo Test "Reconstruction async" Ok.
#####


