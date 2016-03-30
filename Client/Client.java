package Client;

import java.awt.List;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Scanner;

import NameNode.INameNode;
import Protobuf.HDFSProtobuf.BlockLocationRequest;
import Protobuf.HDFSProtobuf.CloseFileRequest;
import Protobuf.HDFSProtobuf.CloseFileResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;
import Protobuf.HDFSProtobuf.OpenFileResponse;


public class Client {
	private static String commandSeperator = "--";
	private static Boolean isSingleCommand = false;
	
	public Client() {
		super();
	}
	
	private  static void put(String fileName) throws MalformedURLException, RemoteException, NotBoundException {
		try {
			OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(fileName);
			openFileRequest.setForRead(true);
			byte[] serializedOpenFileRequest = openFileRequest.build().toByteArray();
			INameNode nameNode = (INameNode)Naming.lookup("NameNode");
			byte [] serializedOpenFileResponse = null;
			serializedOpenFileResponse = nameNode.openFile(serializedOpenFileRequest);
			OpenFileResponse openFileResponse = OpenFileResponse.parseFrom(serializedOpenFileRequest);
			Integer openStatus = openFileResponse.getStatus();
			if(openStatus == 0)
				throw new FileNotFoundException();
			Integer handle = openFileResponse.getHandle();
			//Highly doubtful
			Integer[] blockNums = openFileResponse.getBlockNumsList().toArray(new Integer[0]);
			//I am not sure what to do here. pdf says to use loop but here we can send entire arrays
			for (Integer block : blockNums) {
				BlockLocationRequest.Builder blockLocationRequest = BlockLocationRequest.newBuilder();
				blockLocationRequest.addAllBlockNums(openFileResponse.getBlockNumsList());
				
			}
			CloseFileRequest.Builder closeFileRequest = CloseFileRequest.newBuilder();
			closeFileRequest.setHandle(handle);
			byte [] serializedCloseFileRequest = closeFileRequest.build().toByteArray();
			byte [] serializedCloseFileResponse = null;
			serializedCloseFileResponse = nameNode.closeFile(serializedCloseFileRequest);
			CloseFileResponse closeFileResponse = CloseFileResponse.parseFrom(serializedCloseFileResponse);
			Integer closeStatus = closeFileResponse.getStatus();
			if (closeStatus == 0) 
				throw new IOException();
						
			
		} catch (Exception e) {
			System.err.println(e);
			// TODO: handle exception
		}
	}
	
	private  static void get(String fileName) {
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(false);
		byte[] serializedOpenFileRequest = openFileRequest.build().toByteArray();

		
		
	}
	
	private  static void list() {
		
		
	}
	
	private static void parseCommand(String command) {
//		System.err.println("Executing Command : " + command);
		String[] argumentList = command.split(" ");
		
		String type = argumentList[0]; 
		
		switch (type) {
			case "list":
				try {
					list();	
				} catch (Exception e) {
					System.err.println("list");
				}
				break;
			case "get":
				try {
					get(argumentList[1]);	
				} catch (Exception e) {
					System.err.println("get");
					}
				break;
			case "put":
				try {
					put(argumentList[1]);					
				}
				catch(Exception e) {
					System.err.println("Wrong number of arguments specified\n");					
				}
				break;
			case "exit":
				System.out.println("Exiting...");
				break;
			default:
				System.err.println("Undefined type of command " + type);
		}
		
	}

	public static void main(String[] args) {
		for (String temp : args) {
			if (temp.equals(commandSeperator)) {
				isSingleCommand = true;
			}
		}

		if (isSingleCommand) {
			StringBuilder stringBuilder = new StringBuilder(); 
			Boolean flag = false;
			String sep = "";
			for (String temp : args) {
				if (flag) {
					stringBuilder.append(sep).append(temp);
					sep = " ";
				}
				if (temp.equals(commandSeperator)) {
					flag = true;
				}
			}
			parseCommand(stringBuilder.toString().trim());
		} else {
			// TODO: Go Console Mode
			String command = new String();
			while (!command.equals("exit")) {
				System.out.print(">>> ");
				command = new Scanner(System.in).nextLine();
				if (command.trim().equals("")) 
					continue;
				parseCommand(command.replaceAll("\\s+", " ").trim());
			}
		}
	}

}
