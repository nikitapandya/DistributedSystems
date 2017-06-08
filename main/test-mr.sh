#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
export GOPATH="$here/../../"
echo ""
echo "==> Part I"
export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd "$GOPATH/src/mapreduce"
go test -run Sequential mapreduce/...
echo ""
echo "==> Part II"
(cd "$here" && ./test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
go test -run TestBasic mapreduce/...
echo ""
echo "==> Part IV"
go test -run Failure mapreduce/...
//echo ""
//echo "==> Part V (challenge)"
//(cd "$here" && ./test-ii.sh > /dev/null)

rm "$here"/mrtmp.* "$here"/diff.out
