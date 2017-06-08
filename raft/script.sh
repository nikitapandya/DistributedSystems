export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd $GOPATH
cd src/raft
#go test -run 2A
#go test -run TestBasicAgree2B
#go test -run TestFailAgree2B
#go test -run TestFailNoAgree2B
#go test -run TestConcurrentStarts2B
#go test -run TestRejoin2B
#go test -run TestBackup2B
#go test -run TestCount2B
#go test -run TestPersist12C
#go test -run TestPersist22C
#go test -run TestPersist32C
go test -race -run TestFigure82C
go test -race -run TestUnreliableAgree2C
go test -race -run TestFigure8Unreliable2C
#go test -run TestReliableChurn2C
#go test -run TestUnreliableChurn2C
