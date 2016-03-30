package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.google.protobuf.InvalidProtocolBufferException;

public interface INameNode extends Remote {
	byte[] openFile(byte[] serializedOpenFileRequest) throws RemoteException;
	byte[] closeFile(byte[] serializedCloseFileRequest) throws RemoteException;
	byte[] getBlockLocations(byte[] serializedGetBlockLocationRequest) throws RemoteException;
	byte[] assignBlock(byte[] serializedAssignBlockRequest) throws RemoteException;
	byte[] list(byte[] serializedListFilesRequest) throws RemoteException;
	byte[] blockReport(byte[] serializedBlockReportRequest) throws RemoteException, InvalidProtocolBufferException;
	byte[] heartBeat(byte[] serializedHeartBeatRequest) throws RemoteException, InvalidProtocolBufferException;
}