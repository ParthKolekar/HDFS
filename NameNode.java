import java.rmi.RemoteException;


public class NameNode implements INameNode {
	
	public NameNode() {
		
	}

	public static void main(String[] args) {

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
