.PHONY : all

all : Protobuf Client DataNode NameNode mapred.jar
	make -C Protobuf
	make -C Client
	make -C DataNode
	make -C NameNode
	make -C TaskTracker
	make -C JobTracker

clean : 
	make -C Client clean
	make -C DataNode clean 
	make -C NameNode clean
	make -C TaskTracker clean
	make -C JobTracker clean
	make -C Protobuf clean
	rm -f *.class mapred.jar

Client : 
	make -C Client

DataNode :
	make -C DataNode

NameNode : 
	make -C NameNode

TaskTracker :
	make -C TaskTracker

JobTracker :
	make -C JobTracker

Protobuf :
	make -C Protobuf

mapred.jar : Mapper.class Reducer.class
	jar cvf mapred.jar Mapper.class Reducer.class

%.class : %.java
	javac -cp .:/usr/share/java/protobuf.jar $^

