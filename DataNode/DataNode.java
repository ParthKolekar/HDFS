package DataNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import NameNode.INameNode;
import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;

import com.google.protobuf.InvalidProtocolBufferException;


public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final long serialVersionUID = 1L;
	private static final Integer heartBeatTimeout = 1000;
	private static Integer serverID = -1;
	private static Integer heartBeatID = 0;
	
	
	public DataNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		if (args.length != 1) {
			System.err.println("USAGE: java DataNode.DataNode <nodeID>");
			System.exit(-1);
		}
		
		serverID = Integer.parseInt(args[0]);
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while (true) {
					heartBeatID++;
					HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
					heartBeatRequest.setId(heartBeatID);
					byte[] serializedHeartBeatRequest = heartBeatRequest.build().toByteArray();
					
					INameNode nameNode = null;
					
					try {
						nameNode = (INameNode) Naming.lookup("NameNode");
					} catch (MalformedURLException | RemoteException | NotBoundException e) {
						e.printStackTrace();
					}
					
					byte[] serializedHeartBeatResponse = null;
					
					try {
						serializedHeartBeatResponse = nameNode.heartBeat(serializedHeartBeatRequest);
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

		DataNode dataNode = new DataNode();
		Naming.rebind("DataNode" + serverID.toString(), dataNode);
	}

	@Override
	public byte[] readBlock(byte[] serializedReadBlockRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] writeBlock(byte[] serializedWriteBlockRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}

