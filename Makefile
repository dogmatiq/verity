CGO_ENABLED ?= 1

-include .makefiles/Makefile
-include .makefiles/pkg/protobuf/v1/Makefile
-include .makefiles/pkg/go/v1/Makefile

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"

bank:
	go run ./cmd/bank/main.go

boltdb:
	go test -count=1 ./persistence/provider/boltdb

memory:
	go test -count=1 ./persistence/provider/memory

mysql:
	go test -count=1 ./persistence/provider/sql/mysql

postgres:
	go test -count=1 ./persistence/provider/sql/postgres

sqlite:
	go test -count=1 ./persistence/provider/sql/sqlite

modgraph:
	modgraph --hide draftspecs --hide fixtures --hide cmd | dot  -Tpng -o /tmp/graph.png; open /tmp/graph.png
