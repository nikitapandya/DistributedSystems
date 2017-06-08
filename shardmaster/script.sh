export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd $GOPATH
cd src/shardmaster
go test -race
