export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd "$GOPATH/src/mapreduce"
go test -run Failure mapreduce/...
