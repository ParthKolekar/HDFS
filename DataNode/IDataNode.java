package DataNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {

	byte[] readBlock(byte[] serializedReadBlockRequest) throws RemoteException;

	byte[] writeBlock(byte[] serializedWriteBlockRequest) throws RemoteException;

}