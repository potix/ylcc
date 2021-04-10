#/bin/bash

CGO_LDFLAGS="`mecab-config --libs`" go build
cd client && go build
