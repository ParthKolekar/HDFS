#! /bin/bash

_usage() {
    echo "USAGE: $0 ID"
}

if [[ $# -ne 1 ]]
then
    _usage
    exit
fi

make -sC DataNode

ID="$1"

java -classpath .:/usr/share/java/protobuf.jar DataNode.DataNode $ID 
