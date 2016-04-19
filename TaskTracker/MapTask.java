package TaskTracker;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.jar.JarFile;

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
import com.google.protobuf.InvalidProtocolBufferException;

class MapTask implements Callable<String[]> {

	private final String configurationFile = "Resources/tasktracker.properties";
	private Integer blockID;
	private String mapperName;
	private String taskID;
	private INameNode nameNode;

	public MapTask(Integer blockID, String mapperName, Integer taskID) {
		this.blockID = blockID;
		this.mapperName = mapperName;
		this.taskID = Integer.toString(taskID);

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

		BlockLocationResponse blockLocationResponse = null;
		try {
			blockLocationResponse = BlockLocationResponse.parseFrom(this.nameNode.getBlockLocations(BlockLocationRequest.newBuilder().addBlockNums(this.blockID).build().toByteArray()));
		} catch (InvalidProtocolBufferException | RemoteException e) {
			e.printStackTrace();
		}
		if (blockLocationResponse.getStatus() == 0) {
			// What do I even do?
		}
		ReadBlockResponse readBlockResponse = null;
		for (DataNodeLocation location : blockLocationResponse.getBlockLocations(0).getLocationsList()) {
			readBlockResponse = ReadBlockResponse.parseFrom(((IDataNode) LocateRegistry.getRegistry(location.getIP(), location.getPort()).lookup("DataNode")).readBlock(ReadBlockRequest.newBuilder().setBlockNumber(this.blockID).build().toByteArray()));
			if (readBlockResponse.getStatus() == 0) {
				System.err.println("Error in ReadBlockRequest... Trying next...");
				continue;
			} else {
				break;
			}
		}

		ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
		arrayOutputStream.write(ByteString.copyFrom(readBlockResponse.getDataList()).toByteArray());

		String mapInput = arrayOutputStream.toString();
		File jarFile = new File(mapperName);
		Class[] argTypes = { String.class };
		URLClassLoader child = new URLClassLoader (new URL[] { jarFile.toURI().toURL()}, System.class.getClass().getClassLoader());
		Class<?> classToLoad = Class.forName ("Mapper", true, child);
		Method method = classToLoad.getDeclaredMethod ("map",argTypes);
		Object instance = classToLoad.newInstance ();
		Object result = method.invoke (instance, (Object)mapInput);

		String mapOutput = new String((String) result);

		String fileName = "map-" + this.taskID.toString();

		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(this.nameNode.openFile(OpenFileRequest.newBuilder().setFileName(fileName).setForRead(false).build().toByteArray()));
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

		WriteBlockResponse writeBlockResponse = WriteBlockResponse.parseFrom(((IDataNode) LocateRegistry.getRegistry(location.getIP(), location.getPort()).lookup("DataNode")).writeBlock(WriteBlockRequest.newBuilder().addData(ByteString.copyFromUtf8(mapOutput)).setBlockInfo(BlockLocations.newBuilder().setBlockNumber(assignBlockResponse.getNewBlock().getBlockNumber()).addAllLocations(dataNodeLocations.subList(0, dataNodeLocations.size()))).build().toByteArray()));
		if (writeBlockResponse.getStatus() == 0) {
			System.err.println("Error in WriteBlockRequest...");
		}

		CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(this.nameNode.closeFile(CloseFileRequest.newBuilder().setHandle(handle).build().toByteArray()));
		if (closeFileResponse.getStatus() == 0) {
			System.err.println("Error in CloseFileRequest...");
		}

		String arr[] = new String[2];
		arr[0] = this.taskID;
		arr[1] = fileName;
		return arr;
	}
}
