
export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd $GOPATH
cd src/raft
#go test -race -run 2A
#go test -race -run 2B
#go test -race -run 2C
go test -race
