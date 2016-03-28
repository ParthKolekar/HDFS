package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote {
	byte[] openFile(byte[] openFileRequest) throws RemoteException;
	byte[] closeFile(byte[] closeFileRequest) throws RemoteException;
	byte[] getBlockLocations(byte[] getBlockLocationRequest) throws RemoteException;
	byte[] assignBlock(byte[] assignBlockRequest) throws RemoteException;
	byte[] list(byte[] listFilesRequest) throws RemoteException;
}