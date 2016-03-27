import java.net.MalformedURLException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;


public class NameNode extends UnicastRemoteObject implements INameNode {

	public NameNode() throws RemoteException {
		super();
	}

	public static void main(String[] args) throws RemoteException, MalformedURLException, AlreadyBoundException {
		NameNode nameNode = new NameNode();
		Naming.bind("NameNode", nameNode);
		System.out.println("NameNode bound");
	}

	@Override
	public byte[] openFile(byte[] openFileRequest) throws RemoteException {

		return null;
	}

	@Override
	public byte[] closeFile(byte[] closeFileRequest) throws RemoteException {

		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] getBlockLocationRequest) throws RemoteException {

		return null;
	}

	@Override
	public byte[] assignBlock(byte[] assignBlockRequest) throws RemoteException {

		return null;
	}

	@Override
	public byte[] list(byte[] listFilesRequest) throws RemoteException {

		return null;
	}

}
