#/bin/bash

cd protocol && protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative protocol.proto && cd ..
CGO_LDFLAGS="`mecab-config --libs`" go build
cd client && go build && cd ..
