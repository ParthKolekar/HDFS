#! /bin/bash

_usage() {
    echo "USAGE: $0"
}

make -sC Protobuf
make -sC JobTracker

java -classpath .:/usr/share/java/protobuf.jar JobTracker.JobTracker
