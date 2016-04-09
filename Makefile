.PHONY : all

all : Protobuf Client DataNode NameNode
	make -C Protobuf
	make -C Client
	make -C DataNode
	make -C NameNode

clean : 
	make -C Client clean
	make -C DataNode clean 
	make -C NameNode clean
	make -C Protobuf clean

Client : 
	make -C Client

DataNode :
	make -C DataNode

NameNode : 
	make -C NameNode

Protobuf :
	make -C Protobuf
