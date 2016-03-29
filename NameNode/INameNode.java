package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote {
	byte[] openFile(byte[] serializedOpenFileRequest) throws RemoteException;
	byte[] closeFile(byte[] serializedCloseFileRequest) throws RemoteException;
	byte[] getBlockLocations(byte[] serializedGetBlockLocationRequest) throws RemoteException;
	byte[] assignBlock(byte[] serializedAssignBlockRequest) throws RemoteException;
	byte[] list(byte[] serializedListFilesRequest) throws RemoteException;
}