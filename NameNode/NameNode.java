package NameNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import DataNode.IDataNode;
import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;

import com.google.protobuf.InvalidProtocolBufferException;


public class NameNode extends UnicastRemoteObject implements INameNode {

	private static final long serialVersionUID = 1L;
	private static final Integer heartBeatTimeout = 1000;
	private static Integer heartBeatID = 0;
	private static Integer totalNodes = -1;

	public NameNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
	
		if (args.length != 1) {
			System.err.println("USAGE: java NameNode.NameNode <totalNodes>");
			System.exit(-1);
		}
		
		totalNodes = Integer.parseInt(args[0]);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					heartBeatID++;
					HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
					heartBeatRequest.setId(heartBeatID);
					byte[] serializedHeartBeatRequest = heartBeatRequest.build().toByteArray();
					
					IDataNode dataNode = null;
					
					Integer index = (heartBeatID % totalNodes) + 1;
					
					try {
						dataNode = (IDataNode) Naming.lookup("DataNode" + index.toString());
					} catch (MalformedURLException | RemoteException | NotBoundException e) {
						e.printStackTrace();
					}
					
					byte[] serializedHeartBeatResponse = null;
					
					try {
						serializedHeartBeatResponse = dataNode.heartBeat(serializedHeartBeatRequest);
					} catch (RemoteException | InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
					
					HeartBeatResponse heartBeatResponse = null;
					
					try {
						heartBeatResponse = HeartBeatResponse.parseFrom(serializedHeartBeatResponse);
					} catch (InvalidProtocolBufferException e) {
						e.printStackTrace();
					}
					
					Integer heartBeatStatus = heartBeatResponse.getStatus();
					
					if (heartBeatStatus.equals(heartBeatID)) {
						System.out.println(" Heart Beating... ");
					}
					
					try {
						Thread.sleep(heartBeatTimeout);
					} catch (InterruptedException e) {
						// nope
					}
				}
			}
		}).start();
		
		NameNode nameNode = new NameNode();
		Naming.rebind("NameNode", nameNode);
	}

	@Override
	public byte[] openFile(byte[] serializedOpenFileRequest)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] closeFile(byte[] serializedCloseFileRequest)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] serializedGetBlockLocationRequest)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] assignBlock(byte[] serializedAssignBlockRequest)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] list(byte[] serializedListFilesRequest)
			throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
