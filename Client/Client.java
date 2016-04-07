package Client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.Scanner;

import NameNode.INameNode;
import Protobuf.HDFSProtobuf.BlockLocationRequest;
import Protobuf.HDFSProtobuf.BlockLocationResponse;
import Protobuf.HDFSProtobuf.CloseFileRequest;
import Protobuf.HDFSProtobuf.CloseFileResponse;
import Protobuf.HDFSProtobuf.ListFilesRequest;
import Protobuf.HDFSProtobuf.ListFilesResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;
import Protobuf.HDFSProtobuf.OpenFileResponse;

import com.google.protobuf.InvalidProtocolBufferException;

public class Client {

	private static final String configurationFile = "Resources/client.properties";
	private static String commandSeperator = "--";
	private static Boolean isSingleCommand = false;
	private static String nameNodeLocation;

	private static void get(String fileName) throws MalformedURLException, RemoteException, NotBoundException, InvalidProtocolBufferException {
		INameNode nameNode = (INameNode) Naming.lookup(nameNodeLocation + "NameNode");
		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(nameNode.openFile(OpenFileRequest.newBuilder().setFileName(fileName).setForRead(true).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}
		Integer handle = openFileResponse.getHandle();

		BlockLocationResponse blockLocationResponse = BlockLocationResponse.parseFrom(nameNode.getBlockLocations(BlockLocationRequest.newBuilder().addAllBlockNums(openFileResponse.getBlockNumsList()).build().toByteArray()));
		if (blockLocationResponse.getStatus() == 0) {
			System.err.println("Error in BlockLocationRequest...");
			return;
		}

		System.out.println(blockLocationResponse.getBlockLocationsList());

		CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(nameNode.closeFile(CloseFileRequest.newBuilder().setHandle(handle).build().toByteArray()));
		if (closeFileResponse.getStatus() == 0) {
			System.err.println("Error in CloseFileRequest...");
			return;
		}
	}

	private static void list() throws MalformedURLException, RemoteException, NotBoundException, InvalidProtocolBufferException {
		INameNode nameNode = (INameNode) Naming.lookup(nameNodeLocation + "NameNode");
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

	private static void parseCommand(String command) throws MalformedURLException, RemoteException, NotBoundException, InvalidProtocolBufferException {

		String[] argumentList = command.split(" ", 2);

		String type = argumentList[0];

		switch (type) {
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
			System.err.println("Undefined type of command " + type);
		}

	}

	private static void put(String fileName) throws MalformedURLException, RemoteException, NotBoundException, InvalidProtocolBufferException {

		INameNode nameNode = (INameNode) Naming.lookup(nameNodeLocation + "NameNode");
		OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(nameNode.openFile(OpenFileRequest.newBuilder().setFileName(fileName).setForRead(false).build().toByteArray()));
		if (openFileResponse.getStatus() == 0) {
			System.err.println("Error in OpenFileRequest...");
			return;
		}
		Integer handle = openFileResponse.getHandle();

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
