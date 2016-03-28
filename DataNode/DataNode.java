package DataNode;

import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final long serialVersionUID = 1L;
	private static Integer serverID = -1;
	
	public DataNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		serverID = Integer.parseInt(args[0]);
		DataNode dataNode = new DataNode();
		Naming.bind("DataNode" + serverID.toString(), dataNode);
	}

	@Override
	public byte[] readBlock(byte[] readBlockRequest) throws RemoteException {

		return null;
	}

	@Override
	public byte[] writeBlock(byte[] writeBlockRequest) throws RemoteException {

		return null;
	}

}
