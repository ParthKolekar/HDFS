package NameNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

import Protobuf.HDFSProtobuf.BlockReportRequest;
import Protobuf.HDFSProtobuf.BlockReportResponse;
import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;
import Protobuf.HDFSProtobuf.OpenFileRequest;

public class NameNode extends UnicastRemoteObject implements INameNode {

	private static final long serialVersionUID = 1L;
	private static HashMap<String, Integer> fileNameHandleMap;

	// private static HashMap<Integer, Array<Integer>>

	public static void main(String[] args) throws RemoteException,
			MalformedURLException, AlreadyBoundException {
		NameNode nameNode = new NameNode();
		Naming.rebind("NameNode", nameNode);
	}

	public NameNode() throws RemoteException {
		super();
		fileNameHandleMap = new HashMap<String, Integer>();
	}

	@Override
	public byte[] assignBlock(byte[] serializedAssignBlockRequest) {
		return null;
	}

	@Override
	public byte[] blockReport(byte[] serializedBlockReportRequest) {
		try {
			BlockReportRequest blockReportRequest = BlockReportRequest
					.parseFrom(serializedBlockReportRequest);
			BlockReportResponse.Builder blockReportResonse = BlockReportResponse
					.newBuilder();
			for (Integer temporaryIndex = 0; temporaryIndex < blockReportRequest
					.getBlockNumbersCount(); temporaryIndex++) {
				blockReportResonse.addStatus(1);
			}
			return blockReportResonse.build().toByteArray();
		} catch (Exception e) {
			return BlockReportResponse.newBuilder().addStatus(0).build()
					.toByteArray();
		}
	}

	@Override
	public byte[] closeFile(byte[] serializedCloseFileRequest) {
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] serializedGetBlockLocationRequest) {
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] serializedHeartBeatRequest) {
		try {
			HeartBeatRequest heartBeatRequest = HeartBeatRequest
					.parseFrom(serializedHeartBeatRequest);
			Integer heartBeatID = heartBeatRequest.getId();
			HeartBeatResponse.Builder heartBeatRespose = HeartBeatResponse
					.newBuilder();
			heartBeatRespose.setStatus(heartBeatID);
			byte[] serializedHeartBeatResponse = heartBeatRespose.build()
					.toByteArray();
			return serializedHeartBeatResponse;
		} catch (Exception e) {
			return HeartBeatResponse.newBuilder().setStatus(0).build()
					.toByteArray();
		}
	}

	@Override
	public byte[] list(byte[] serializedListFilesRequest) {
		return null;
	}

	@Override
	public byte[] openFile(byte[] serializedOpenFileRequest) {
		byte[] serializedOpenFileResponse = null;
		try {
			OpenFileRequest openFileRequest = OpenFileRequest
					.parseFrom(serializedOpenFileRequest);
			String fileName = openFileRequest.getFileName();
			Boolean forRead = openFileRequest.getForRead();

			if (forRead) {

			} else {

			}

		} catch (Exception e) {
			// TODO: handle exception
		}

		return serializedOpenFileResponse;
	}

}
