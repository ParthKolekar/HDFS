#! /bin/bash

_usage() {
    echo "USAGE: $0"
}

make -sC Protobuf
make -sC NameNode

java -classpath .:/usr/share/java/protobuf.jar NameNode.NameNode
