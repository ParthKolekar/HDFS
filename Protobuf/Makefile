all : HDFSProtobuf.class MapReduceProtobuf.class

%.class : %.java
	 javac -classpath ..:/usr/share/java/protobuf.jar $^

HDFSProtobuf.java : HDFS.proto
	protoc --java_out=.. HDFS.proto

MapReduceProtobuf.java : MapReduce.proto
	protoc --java_out=.. MapReduce.proto

clean:
	rm -f *.class  HDFSProtobuf.java MapReduceProtobuf.java
