package Client;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

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
import Protobuf.HDFSProtobuf.ListFilesRequest;
import Protobuf.HDFSProtobuf.ListFilesResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;
import Protobuf.HDFSProtobuf.OpenFileResponse;
import Protobuf.HDFSProtobuf.ReadBlockRequest;
import Protobuf.HDFSProtobuf.ReadBlockResponse;
import Protobuf.HDFSProtobuf.WriteBlockRequest;
import Protobuf.HDFSProtobuf.WriteBlockResponse;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class Client {

	private static final String configurationFile = "Resources/client.properties";
	private static String commandSeperator = "--";
	private static Boolean isSingleCommand = false;
	private static String nameNodeLocation;

	private static void get(String fileName) throws NotBoundException, IOException {

		INameNode nameNode = (INameNode) LocateRegistry.getRegistry(nameNodeLocation, Registry.REGISTRY_PORT).lookup("NameNode");

		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(nameNode.openFile(OpenFileRequest.newBuilder().setFileName(fileName).setForRead(true).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}

		BlockLocationResponse blockLocationResponse = BlockLocationResponse.parseFrom(nameNode.getBlockLocations(BlockLocationRequest.newBuilder().addAllBlockNums(openFileResponse.getBlockNumsList()).build().toByteArray()));
		if (blockLocationResponse.getStatus() == 0) {
			System.err.println("Error in BlockLocationRequest...");
			return;
		}

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

		for (BlockLocations tempBlockLocations : blockLocationResponse.getBlockLocationsList()) {
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
				return;
			}
			byteArrayOutputStream.write(ByteString.copyFrom(readBlockResponse.getDataList()).toByteArray());
		}

		Files.write(Paths.get(fileName), byteArrayOutputStream.toByteArray());

		CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(nameNode.closeFile(CloseFileRequest.newBuilder().setHandle(openFileResponse.getHandle()).build().toByteArray()));
		if (closeFileResponse.getStatus() == 0) {
			System.err.println("Error in CloseFileRequest...");
			return;
		}

	}

	private static void list() throws MalformedURLException, RemoteException, NotBoundException, InvalidProtocolBufferException {

		INameNode nameNode = (INameNode) LocateRegistry.getRegistry(nameNodeLocation, Registry.REGISTRY_PORT).lookup("NameNode");

		ListFilesResponse listFilesResponse = ListFilesResponse.parseFrom(nameNode.list(ListFilesRequest.newBuilder().build().toByteArray()));
		if (listFilesResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}

		System.out.println(listFilesResponse.getFileNamesList());
		// TODO: Prettify

	}

	public static void main(String[] args) throws IOException, NotBoundException {

		Properties properties = new Properties();
		InputStream inputStream = new FileInputStream(configurationFile);
		properties.load(inputStream);

		nameNodeLocation = properties.getProperty("NameNode Location");

		if (nameNodeLocation == null) {
			System.out.println("Configuration Missing...");
			System.exit(-1);
		}

		for (String temp : args) {
			if (temp.equals(commandSeperator)) {
				isSingleCommand = true;
			}
		}

		if (isSingleCommand) {
			StringBuilder stringBuilder = new StringBuilder();
			Boolean flag = false;
			String separator = "";
			for (String temp : args) {
				if (flag) {
					stringBuilder.append(separator).append(temp);
					separator = " ";
				}
				if (temp.equals(commandSeperator)) {
					flag = true;
				}
			}
			parseCommand(stringBuilder.toString().trim());
		} else {
			String command = new String();
			while (!command.equals("exit")) {
				System.out.print(">>> ");
				command = new Scanner(System.in).nextLine();
				if (command.trim().equals("")) {
					continue;
				}
				parseCommand(command.replaceAll("\\s+", " ").trim());
			}
		}

	}

	private static void parseCommand(String command) throws NotBoundException, IOException {

		String[] argumentList = command.split(" ", 2);

		switch (argumentList[0]) {
		case "list":
			list();
			break;
		case "get":
			if (argumentList.length <= 1) {
				System.err.println("No FileName Given...");
			} else {
				get(argumentList[1]);
			}
			break;
		case "put":
			if (argumentList.length <= 1) {
				System.err.println("No FileName Given...");
			} else {
				put(argumentList[1]);
			}
			break;
		case "exit":
			System.out.println("Exiting...");
			break;
		default:
			System.err.println("Undefined type of command " + argumentList[0]);
		}

	}

	private static void put(String fileName) throws NotBoundException, IOException {

		INameNode nameNode = (INameNode) LocateRegistry.getRegistry(nameNodeLocation, Registry.REGISTRY_PORT).lookup("NameNode");

		Path path = Paths.get(fileName);

		if (!Files.exists(path)) {
			System.err.println("No Such File " + fileName);
			return;
		}

		if (!Files.isReadable(path)) {
			System.err.println("Can't read File " + fileName);
			return;
		}

		if (Files.isDirectory(path)) {
			System.err.println("Is A Directory... " + fileName);
			return;
		}

		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(nameNode.openFile(OpenFileRequest.newBuilder().setFileName(path.getFileName().toString()).setForRead(false).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}

		Integer handle = openFileResponse.getHandle();
		InputStream inputStream = Files.newInputStream(path);
		byte[] byteBuffer = new byte[32000000];
		Integer bytesRead;

		while ((bytesRead = inputStream.read(byteBuffer)) != -1) {
			AssignBlockResponse assignBlockResponse = AssignBlockResponse.parseFrom(nameNode.assignBlock(AssignBlockRequest.newBuilder().setHandle(handle).build().toByteArray()));
			if (assignBlockResponse.getStatus() == 0) {
				System.err.println("Error in AssignBlockRequest...");
				return;
			}

			BlockLocations blockLocations = assignBlockResponse.getNewBlock();
			Integer blockNumber = blockLocations.getBlockNumber();
			List<DataNodeLocation> dataNodeLocations = blockLocations.getLocationsList();
			DataNodeLocation location = dataNodeLocations.get(0);

			IDataNode dataNode = (IDataNode) LocateRegistry.getRegistry(location.getIP(), location.getPort()).lookup("DataNode");

			WriteBlockResponse writeBlockResponse = WriteBlockResponse.parseFrom(dataNode.writeBlock(WriteBlockRequest.newBuilder().addData(ByteString.copyFrom(bytesRead == 32000000 ? byteBuffer : Arrays.copyOf(byteBuffer, bytesRead))).setBlockInfo(BlockLocations.newBuilder().setBlockNumber(blockNumber).addAllLocations(dataNodeLocations.subList(0, dataNodeLocations.size()))).build().toByteArray()));
			if (writeBlockResponse.getStatus() == 0) {
				System.err.println("Error in WriteBlockRequest...");
				return;
			}
		}

		CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(nameNode.closeFile(CloseFileRequest.newBuilder().setHandle(handle).build().toByteArray()));
		if (closeFileResponse.getStatus() == 0) {
			System.err.println("Error in CloseFileRequest...");
			return;
		}

	}

	public Client() {
		super();
	}

}
