import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote {
	byte[] readBlock(byte[] readBlockRequest) throws RemoteException;
	byte[] writeBlock(byte[] writeBlockRequest) throws RemoteException;
}
