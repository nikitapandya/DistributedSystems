export GOROOT=/research/sesa/451/go
export PATH=$PATH:$GOROOT/bin
export GOPATH=$HOME/451
cd $GOPATH
cd src/kvraft
go test -run TestBasic
go test -run TestConcurrent
go test -run TestUnreliable
go test -run TestUnreliableOneKey
go test -run TestOnePartition
go test -run TestManyPartitionsOneClient
go test -run TestManyPartitionsManyClients
go test -run TestPersistOneClient
go test -run TestPersistConcurrent
go test -run TestPersistConcurrentUnreliable
go test -run TestPersistPartition
go test -run TestPersistPartitionUnreliable
#go test -run TestPersistPartitionUnreliable
