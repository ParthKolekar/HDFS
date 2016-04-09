#! /bin/bash

_usage() {
    echo "USAGE: $0"
}

make -sC Client

java -classpath .:/usr/share/java/protobuf.jar Client.Client $@

