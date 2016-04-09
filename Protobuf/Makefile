HDFSProtobuf.class : HDFSProtobuf.java
	 javac -classpath ..:/usr/share/java/protobuf.jar HDFSProtobuf.java

HDFSProtobuf.java : HDFS.proto
	protoc --java_out=.. HDFS.proto

clean:
	rm -f *.class  HDFSProtobuf.java 
