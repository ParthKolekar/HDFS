package JobTracker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import Protobuf.MapReduceProtobuf.HeartBeatRequest;
import Protobuf.MapReduceProtobuf.HeartBeatResponse;
import Protobuf.MapReduceProtobuf.JobStatusRequest;
import Protobuf.MapReduceProtobuf.JobStatusResponse;
import Protobuf.MapReduceProtobuf.JobSubmitRequest;
import Protobuf.MapReduceProtobuf.JobSubmitResponse;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private class Job {
		private String reducerName;
		private String mapperName;
		private String inputFile;
		private String outputFile;
		private Integer totalMappers;
		private Integer totalReducers;
		private Integer mapTasksStarted;
		private Integer reduceTasksStarted;
		private Boolean isDone;

		Job() {
			super();
		}

		public String getInputFile() {
			return this.inputFile;
		}

		public Boolean getIsDone() {
			return this.isDone;
		}

		public String getMapperName() {
			return this.mapperName;
		}

		public Integer getMapTasksStarted() {
			return this.mapTasksStarted;
		}

		public String getOutputFile() {
			return this.outputFile;
		}

		public String getReducerName() {
			return this.reducerName;
		}

		public Integer getReduceTasksStarted() {
			return this.reduceTasksStarted;
		}

		public Integer getTotalMappers() {
			return this.totalMappers;
		}

		public Integer getTotalReducers() {
			return this.totalReducers;
		}

		public void setInputFile(String inputFile) {
			this.inputFile = inputFile;
		}

		public void setIsDone(Boolean isDone) {
			this.isDone = isDone;
		}

		public void setMapperName(String mapperName) {
			this.mapperName = mapperName;
		}

		public void setMapTasksStarted(Integer mapTasksStarted) {
			this.mapTasksStarted = mapTasksStarted;
		}

		public void setOutputFile(String outputFile) {
			this.outputFile = outputFile;
		}

		public void setReducerName(String reducerName) {
			this.reducerName = reducerName;
		}

		public void setReduceTasksStarted(Integer reduceTasksStarted) {
			this.reduceTasksStarted = reduceTasksStarted;
		}

		public void setTotalMappers(Integer totalMappers) {
			this.totalMappers = totalMappers;
		}

		public void setTotalReducers(Integer totalReducers) {
			this.totalReducers = totalReducers;
		}
	}

	private static final long serialVersionUID = 1L;

	private static final String configurationFile = "Resources/jobtracker.properties";
	private static String networkInterface;
	private static HashMap<Integer, Job> jobsList;

	private static Integer jobID = 0;

	private static Integer getNewJobID() {
		jobID++;
		return new Integer(jobID);
	}

	public static void main(String[] args) throws IOException {

		Properties properties = new Properties();
		InputStream inputStream = new FileInputStream(configurationFile);
		properties.load(inputStream);

		networkInterface = properties.getProperty("Network Interface");

		if (networkInterface == null) {
			System.out.println("Configuration Missing...");
			System.exit(-1);
		}

		Inet4Address inetAddress = null;
		try {
			Enumeration<InetAddress> enumeration = NetworkInterface.getByName(networkInterface).getInetAddresses();
			while (enumeration.hasMoreElements()) {
				InetAddress tempInetAddress = enumeration.nextElement();
				if (tempInetAddress instanceof Inet4Address) {
					inetAddress = (Inet4Address) tempInetAddress;
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
		}
		if (inetAddress == null) {
			System.err.println("Error Obtaining Network Information");
			System.exit(-1);
		}

		System.setProperty("java.rmi.server.hostname", inetAddress.getHostAddress());
		try {
			LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
		} catch (ExportException e) {
			System.err.println("Using existing registry...");
		}
		LocateRegistry.getRegistry(inetAddress.getHostAddress(), Registry.REGISTRY_PORT).rebind("JobTracker", new JobTracker());

		System.out.println("Loaded JobTracker...");
	}

	public JobTracker() throws RemoteException {
		super();
	}

	@Override
	public byte[] getJobStatus(byte[] serializedJobStatusRequest) {
		try {
			Job job = jobsList.get(JobStatusRequest.parseFrom(serializedJobStatusRequest).getJobId());
			return JobStatusResponse.newBuilder().setStatus(1).setJobDone(job.getIsDone()).setNumMapTasksStarted(job.getMapTasksStarted()).setNumReduceTasksStarted(job.getReduceTasksStarted()).setTotalMapTasks(job.getTotalMappers()).setTotalReduceTasks(job.getTotalReducers()).build().toByteArray();
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			return JobStatusResponse.newBuilder().setStatus(0).build().toByteArray();
		}
	}

	@Override
	public byte[] heartBeat(byte[] serializedHeartBeatRequest) {
		try {
			HeartBeatRequest heartBeatRequest = HeartBeatRequest.parseFrom(serializedHeartBeatRequest);
			return null;
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
			return HeartBeatResponse.newBuilder().setStatus(0).build().toByteArray();
		}
	}

	@Override
	public byte[] jobSubmit(byte[] serializedJobSubmitRequest) {
		try {
			JobSubmitRequest jobSubmitRequest = JobSubmitRequest.parseFrom(serializedJobSubmitRequest);
			Integer jobID = getNewJobID();
			Job job = new Job();
			job.setMapperName(jobSubmitRequest.getMapperName());
			job.setReducerName(jobSubmitRequest.getReducerName());
			job.setInputFile(jobSubmitRequest.getInputFile());
			job.setOutputFile(jobSubmitRequest.getOutputFile());
			job.setTotalReducers(jobSubmitRequest.getNumReduceTasks());
			jobsList.put(jobID, job);
			return JobSubmitResponse.newBuilder().setStatus(1).setJobId(jobID).build().toByteArray();
		} catch (InvalidProtocolBufferException e) {
			return JobSubmitResponse.newBuilder().setStatus(0).build().toByteArray();
		}
	}
}
