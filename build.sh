#/bin/bash

CGO_LDFLAGS="`mecab-config --libs`" go build
