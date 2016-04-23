package TaskTracker;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import DataNode.IDataNode;
import NameNode.INameNode;
import Protobuf.HDFSProtobuf.AssignBlockRequest;
import Protobuf.HDFSProtobuf.AssignBlockResponse;
import Protobuf.HDFSProtobuf.BlockLocationRequest;
import Protobuf.HDFSProtobuf.BlockLocationResponse;
import Protobuf.HDFSProtobuf.BlockLocations;
import Protobuf.HDFSProtobuf.CloseFileRequest;
import Protobuf.HDFSProtobuf.CloseFileResponse;
import Protobuf.HDFSProtobuf.DataNodeLocation;
import Protobuf.HDFSProtobuf.OpenFileRequest;
import Protobuf.HDFSProtobuf.OpenFileResponse;
import Protobuf.HDFSProtobuf.ReadBlockRequest;
import Protobuf.HDFSProtobuf.ReadBlockResponse;
import Protobuf.HDFSProtobuf.WriteBlockRequest;
import Protobuf.HDFSProtobuf.WriteBlockResponse;

import com.google.protobuf.ByteString;

class ReduceTask implements Callable<String[]> {

	private final String configurationFile = "Resources/tasktracker.properties";
	private String taskID;
	private String reducerName;
	private ArrayList<String> mapOutputFilesList;
	private String outputFile;
	private INameNode nameNode;

	public ReduceTask(List<String> mapOutputFilesList, String reducerName, String outputFile, Integer taskID) {
		this.taskID = Integer.toString(taskID);
		this.reducerName = reducerName;
		this.mapOutputFilesList = new ArrayList<String>(mapOutputFilesList);
		this.outputFile = outputFile;

		Properties properties = new Properties();
		InputStream inputStream = null;
		try {
			inputStream = new FileInputStream(this.configurationFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			this.nameNode = (INameNode) LocateRegistry.getRegistry(properties.getProperty("NameNode Location"), Registry.REGISTRY_PORT).lookup("NameNode");
		} catch (RemoteException | NotBoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public String[] call() throws Exception {

		ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();

		for (String fileName : this.mapOutputFilesList) {
			OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(this.nameNode.openFile(OpenFileRequest.newBuilder().setFileName(fileName).setForRead(true).build().toByteArray()));
			if (openFileResponse.getStatus() == 0) {
				System.err.println("Error in OpenFileRequest...");
			}

			BlockLocationResponse blockLocationResponse = BlockLocationResponse.parseFrom(this.nameNode.getBlockLocations(BlockLocationRequest.newBuilder().addAllBlockNums(openFileResponse.getBlockNumsList()).build().toByteArray()));
			if (blockLocationResponse.getStatus() == 0) {
				System.err.println("Error in BlockLocationRequest...");
			}

			BlockLocations tempBlockLocations = blockLocationResponse.getBlockLocations(0);
			ReadBlockResponse readBlockResponse = null;
			for (DataNodeLocation location : tempBlockLocations.getLocationsList()) {
				readBlockResponse = ReadBlockResponse.parseFrom(((IDataNode) LocateRegistry.getRegistry(location.getIP(), location.getPort()).lookup("DataNode")).readBlock(ReadBlockRequest.newBuilder().setBlockNumber(tempBlockLocations.getBlockNumber()).build().toByteArray()));
				if (readBlockResponse.getStatus() == 0) {
					System.err.println("Error in ReadBlockRequest... Trying next...");
					continue;
				} else {
					break;
				}
			}
			if (readBlockResponse.getStatus() == 0) {
				System.err.println("No more DataNodes... Failing...");
			}

			arrayOutputStream.write(ByteString.copyFrom(readBlockResponse.getDataList()).toByteArray());
		}

		Class<?>[] argTypes = { String.class };
		Class<?> classToLoad = Class.forName("Reducer", true, new URLClassLoader(new URL[] { new File(this.reducerName).toURI().toURL() }, System.class.getClass().getClassLoader()));
		String totalReduceOutput = new String((String) classToLoad.getDeclaredMethod("reduce", argTypes).invoke(classToLoad.newInstance(), (Object) arrayOutputStream.toString()));

		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(this.nameNode.openFile(OpenFileRequest.newBuilder().setFileName(this.outputFile).setForRead(false).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
		}

		Integer handle = openFileResponse.getHandle();

		AssignBlockResponse assignBlockResponse = AssignBlockResponse.parseFrom(this.nameNode.assignBlock(AssignBlockRequest.newBuilder().setHandle(handle).build().toByteArray()));
		if (assignBlockResponse.getStatus() == 0) {
			System.err.println("Error in AssignBlockRequest...");
		}

		List<DataNodeLocation> dataNodeLocations = assignBlockResponse.getNewBlock().getLocationsList();
		DataNodeLocation location = dataNodeLocations.get(0);

		WriteBlockResponse writeBlockResponse = WriteBlockResponse.parseFrom(((IDataNode) LocateRegistry.getRegistry(location.getIP(), location.getPort()).lookup("DataNode")).writeBlock(WriteBlockRequest.newBuilder().addData(ByteString.copyFromUtf8(totalReduceOutput)).setBlockInfo(BlockLocations.newBuilder().setBlockNumber(assignBlockResponse.getNewBlock().getBlockNumber()).addAllLocations(dataNodeLocations.subList(0, dataNodeLocations.size()))).build().toByteArray()));
		if (writeBlockResponse.getStatus() == 0) {
			System.err.println("Error in WriteBlockRequest...");
		}

		CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(this.nameNode.closeFile(CloseFileRequest.newBuilder().setHandle(handle).build().toByteArray()));
		if (closeFileResponse.getStatus() == 0) {
			System.err.println("Error in CloseFileRequest...");
		}

		String arr[] = new String[1];
		arr[0] = this.taskID;
		return arr;
	}
}