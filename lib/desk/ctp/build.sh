#!/bin/bash
# Written by Suzhengchun on 20160213

set -e
BUILDDIR=build
rm -rf $BUILDDIR
if [ ! -f $BUILDDIR ]; then
    mkdir -p $BUILDDIR
fi
pushd $BUILDDIR
cmake ..
make VERBOSE=1 -j 1
ln -fs `pwd`/lib/vnctpmd.so ../vnctpmd/test/vnctpmd.so
ln -fs `pwd`/lib/vnctptd.so ../vnctptd/test/vnctptd.so
cp ../vnctpmd/test/vnctpmd.* ../../../trader/ctp_trader/ctp_gateway/
cp ../vnctptd/test/vnctptd.* ../../../trader/ctp_trader/ctp_gateway/
popd
