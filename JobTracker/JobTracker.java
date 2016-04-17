package JobTracker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import NameNode.INameNode;
import Protobuf.HDFSProtobuf.BlockLocationRequest;
import Protobuf.HDFSProtobuf.BlockLocationResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;
import Protobuf.HDFSProtobuf.OpenFileResponse;
import Protobuf.MapReduceProtobuf.BlockLocations;
import Protobuf.MapReduceProtobuf.DataNodeLocation;
import Protobuf.MapReduceProtobuf.HeartBeatRequest;
import Protobuf.MapReduceProtobuf.HeartBeatResponse;
import Protobuf.MapReduceProtobuf.JobStatusRequest;
import Protobuf.MapReduceProtobuf.JobStatusResponse;
import Protobuf.MapReduceProtobuf.JobSubmitRequest;
import Protobuf.MapReduceProtobuf.JobSubmitResponse;
import Protobuf.MapReduceProtobuf.MapTaskInfo;
import Protobuf.MapReduceProtobuf.ReducerTaskInfo;

import com.google.protobuf.InvalidProtocolBufferException;

public class JobTracker extends UnicastRemoteObject implements IJobTracker {

	private static final long serialVersionUID = 1L;

	private static final String configurationFile = "Resources/jobtracker.properties";
	private static String networkInterface;
	private static HashMap<Integer, Job> jobList;
	private static HashMap<Integer, MapTaskInfo> mapTaskList;
	private static HashMap<Integer, ReducerTaskInfo> reduceTaskList;
	private static HashMap<Integer, DataNodeLocation> taskTrackerIDDataNodeLocationMap;
	private static Integer jobID = 0;
	private static Integer taskID = 0;
	private static INameNode nameNode;

	private static void createTask(Integer jobId) throws InvalidProtocolBufferException, RemoteException {
		Job job = jobList.get(jobId);

		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(nameNode.openFile(OpenFileRequest.newBuilder().setFileName(job.getInputFile()).setForRead(true).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}

		BlockLocationResponse blockLocationResponse = BlockLocationResponse.parseFrom(nameNode.getBlockLocations(BlockLocationRequest.newBuilder().addAllBlockNums(openFileResponse.getBlockNumsList()).build().toByteArray()));
		if (blockLocationResponse.getStatus() == 0) {
			System.err.println("Error in BlockLocationRequest...");
			return;
		}

		for (Protobuf.HDFSProtobuf.BlockLocations tempBlockLocations : blockLocationResponse.getBlockLocationsList()) {
			Integer taskID = getNewTaskID();
			MapTaskInfo.Builder mapTaskInfo = MapTaskInfo.newBuilder();
			mapTaskInfo.setJobId(jobID).setTaskId(taskID).setMapName(job.getMapperName());
			mapTaskInfo.addInputBlocks(BlockLocations.parseFrom(tempBlockLocations.toByteArray()));
			mapTaskList.put(taskID, mapTaskInfo.build());
		}
	}

	private static Integer getNewJobID() {
		jobID++;
		return new Integer(jobID);
	}

	private static Integer getNewTaskID() {
		taskID++;
		return new Integer(taskID);
	}

	public static void main(String[] args) throws IOException, NotBoundException {

		Properties properties = new Properties();
		InputStream inputStream = new FileInputStream(configurationFile);
		properties.load(inputStream);

		networkInterface = properties.getProperty("Network Interface");
		nameNode = (INameNode) LocateRegistry.getRegistry(properties.getProperty("NameNode Location"), Registry.REGISTRY_PORT).lookup("NameNode");

		if ((networkInterface == null) || (nameNode == null)) {
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
			Job job = jobList.get(JobStatusRequest.parseFrom(serializedJobStatusRequest).getJobId());
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
			Integer mapSlotsFree = heartBeatRequest.getNumMapSlotsFree();
			Integer reduceSlotsFree = heartBeatRequest.getNumReduceSlotsFree();

			for (Integer tempInteger = 0; tempInteger < mapSlotsFree; tempInteger++) {

			}

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
			jobList.put(jobID, job);
			createTask(jobID);
			return JobSubmitResponse.newBuilder().setStatus(1).setJobId(jobID).build().toByteArray();
		} catch (InvalidProtocolBufferException | RemoteException e) {
			e.printStackTrace();
			return JobSubmitResponse.newBuilder().setStatus(0).build().toByteArray();
		}
	}
}
