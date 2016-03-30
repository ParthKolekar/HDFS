package DataNode;

import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import NameNode.INameNode;
import Protobuf.HDFSProtobuf.BlockLocations;
import Protobuf.HDFSProtobuf.HeartBeatRequest;
import Protobuf.HDFSProtobuf.HeartBeatResponse;
import Protobuf.HDFSProtobuf.ReadBlockRequest;
import Protobuf.HDFSProtobuf.ReadBlockResponse;
import Protobuf.HDFSProtobuf.WriteBlockRequest;
import Protobuf.HDFSProtobuf.WriteBlockResponse;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;


public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final long serialVersionUID = 1L;
	private static final Integer heartBeatTimeout = 1000;
	private static Integer heartBeatID = 0;
	
	
	public DataNode() throws RemoteException {
		super();
	}
	
	public void check() throws RemoteException, InvalidProtocolBufferException
	{
	    ReadBlockRequest.Builder readBlockRequest = ReadBlockRequest.newBuilder();
	    readBlockRequest.setBlockNumber(1);
	    
	    byte[] serializedReadBlockRequest = readBlockRequest.build().toByteArray();
//	    DataNode a = new DataNode();
	    byte [] response = this.readBlock(serializedReadBlockRequest);
	    ReadBlockResponse readBlockResponse = ReadBlockResponse.parseFrom(response);
	    System.out.println(readBlockResponse.getStatus());
	    
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		
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
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				
				
			}
		}).start();
		
		
		DataNode dataNode = new DataNode();
		try {
			dataNode.check();
//			Path path = Paths.get(Integer.toString(1));
//		    byte[] data = Files.readAllBytes(path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e);
		}
		Naming.rebind("DataNode", dataNode);
	}

	@Override
	public byte[] readBlock(byte[] serializedReadBlockRequest) throws RemoteException {
		// TODO Auto-generated method stub
		byte[] serializedReadBlockResponse = null;
		try {
			ReadBlockRequest readBlockRequest = ReadBlockRequest.parseFrom(serializedReadBlockRequest);
			Integer blockNumber = readBlockRequest.getBlockNumber();
			Path path = Paths.get(Integer.toString(blockNumber));
		    byte[] data = Files.readAllBytes(path);
		    ReadBlockResponse.Builder readBlockResponse = ReadBlockResponse.newBuilder();
		    readBlockResponse.addData(ByteString.copyFrom(data));
		    readBlockResponse.setStatus(1);
		    serializedReadBlockResponse = readBlockResponse.build().toByteArray();
		} catch (Exception e) {
			return ReadBlockResponse.newBuilder().setStatus(0).build().toByteArray();
			// TODO: handle exception
		}
		return serializedReadBlockResponse;
	}

	@Override
	public byte[] writeBlock(byte[] serializedWriteBlockRequest) throws RemoteException {
		byte [] serializedWriteBlockResponse = null;
		try {
			WriteBlockRequest writeBlockRequest =  WriteBlockRequest.parseFrom(serializedWriteBlockRequest);
			ByteString data = ByteString.copyFrom(writeBlockRequest.getDataList());
			BlockLocations blockLocations = writeBlockRequest.getBlockInfo();
			Integer blockNumber = blockLocations.getBlockNumber();
			Path path = Paths.get(Integer.toString(blockNumber));//check for file creation
			Files.write(path, data.toByteArray());
			
		    WriteBlockResponse.Builder writeBlockResponse = WriteBlockResponse.newBuilder();
		    writeBlockResponse.setStatus(1);
		    serializedWriteBlockResponse = writeBlockResponse.build().toByteArray();
				
		} catch (Exception e) {
			return (WriteBlockResponse.newBuilder().setStatus(0)).build().toByteArray();
			// TODO: handle exception
		}
		// TODO Auto-generated method stub
		return serializedWriteBlockResponse;
	}

}

