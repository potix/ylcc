module github.com/potix/ylcc/client/example

go 1.16

require github.com/potix/ylcc/client v0.0.0

require (
	github.com/potix/ylcc/protocol v0.0.0
	google.golang.org/grpc v1.37.0
)

replace (
	github.com/potix/ylcc/client v0.0.0 => ../
	github.com/potix/ylcc/protocol v0.0.0 => ../../protocol
)
