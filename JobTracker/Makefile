all : Job.class JobTracker.class

Job.class : Job.java
	 javac -classpath ..:/usr/share/java/protobuf.jar Job.java

JobTracker.class : JobTracker.java
	 javac -classpath ..:/usr/share/java/protobuf.jar JobTracker.java

clean:
	rm -f *.class
