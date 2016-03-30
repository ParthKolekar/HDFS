package NameNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import Protobuf.HDFSProtobuf.BlockReportRequest;
import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;

import com.google.protobuf.InvalidProtocolBufferException;


public class NameNode extends UnicastRemoteObject implements INameNode {

	private static final long serialVersionUID = 1L;
	private static HashMap<String, Integer> fileNameHandleMap;
	private static HashMap<Integer, Array<Integer>>

	public NameNode() throws RemoteException {
		super();
		fileNameHandleMap = new HashMap<String, Integer>();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		NameNode nameNode = new NameNode();
		Naming.rebind("NameNode", nameNode);
	}

	@Override
	public byte[] openFile(byte[] serializedOpenFileRequest) throws RemoteException {
		// TODO Auto-generated method stub
		byte[] serializedOpenFileResponse = null;
		try {
			OpenFileRequest openFileRequest = OpenFileRequest.parseFrom(serializedOpenFileRequest);
			String fileName = openFileRequest.getFileName();
			Boolean forRead  = openFileRequest.getForRead();
						
			if (forRead) {
				
			} else {
				
			}
				
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		return serializedOpenFileResponse;
	}

	@Override
	public byte[] closeFile(byte[] serializedCloseFileRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] serializedGetBlockLocationRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] assignBlock(byte[] serializedAssignBlockRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] list(byte[] serializedListFilesRequest) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] blockReport(byte[] serializedBlockReportRequest) throws RemoteException, InvalidProtocolBufferException {
		BlockReportRequest blockReportRequest = BlockReportRequest.parseFrom(serializedBlockReportRequest);
		
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
