CGO_ENABLED ?= 1
GO_RACE_DETECTION = false

-include .makefiles/Makefile
-include .makefiles/pkg/protobuf/v2/Makefile
-include .makefiles/pkg/go/v1/Makefile

.makefiles/%:
	@curl -sfL https://makefiles.dev/v1 | bash /dev/stdin "$@"

boltdb:
	go test -count=1 ./persistence/boltpersistence

memory:
	go test -count=1 ./persistence/memorypersistence

mysql:
	go test -count=1 ./persistence/sqlpersistence/mysql

postgres:
	go test -count=1 ./persistence/sqlpersistence/postgres

sqlite:
	go test -count=1 ./persistence/sqlpersistence/sqlite

modgraph:
	modgraph \
		--hide-shallow . \
		--hide draftspecs \
		--hide fixtures \
		--hide cmd | dot  -Tpng -o /tmp/graph.png; open /tmp/graph.png
