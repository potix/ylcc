# ylcc
youtube live chat collector

# api 
used grpc.

# config
config file is toml format.

# dependency
- mecab

```
# apt install mecab
# CGO_LDFLAGS="`mecab-config --libs`" go get github.com/shogo82148/go-mecab
```

- protocol buffer
  - see https://developers.google.com/protocol-buffers/docs/gotutorial#compiling-your-protocol-buffers

# build 

```
# ./build.sh
```
# run

```
./ylcc 
```

# Apache License 2.0
This software includes https://github.com/psykhi/wordclouds.
You may obtain a copy of the License at "http://www.apache.org/licenses/LICENSE-2.0".
Please be noted that a portion of this software is made by changing or modifying original source files.

