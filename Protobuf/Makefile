.PHONY : clean all

HDFSProtobuf.java : HDFS.proto
	protoc --java_out=.. HDFS.proto

clean:
	$(RM) *.class  HDFSProtobuf.java 
