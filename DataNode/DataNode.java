package DataNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;

import com.google.protobuf.InvalidProtocolBufferException;


public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final long serialVersionUID = 1L;
	private static Integer serverID = -1;
	private static Integer totalNodes = -1;
	
	public DataNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		if (args.length != 2) {
			System.err.println("USAGE: java DataNode.DataNode <nodeID> <totalNodes>");
			System.exit(-1);
		}
		serverID = Integer.parseInt(args[0]);
		totalNodes = Integer.parseInt(args[1]);
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

	@Override
	public byte[] heartBeat(byte[] serializedHeartBeatRequest) throws RemoteException, InvalidProtocolBufferException {
		HeartBeatRequest heartBeatRequest = HeartBeatRequest.parseFrom(serializedHeartBeatRequest);
		Integer heartBeatID = heartBeatRequest.getId();
		HeartBeatResponse.Builder heartBeatRespose = HeartBeatResponse.newBuilder();
		heartBeatRespose.setStatus(heartBeatID);
		byte[] serializedHeartBeatResponse = heartBeatRespose.build().toByteArray();
		return serializedHeartBeatResponse;
	}

}
