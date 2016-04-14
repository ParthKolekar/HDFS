package JobTracker;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IJobTracker extends Remote {
	byte[] getJobStatus(byte[] serializedJobStatusRequest) throws RemoteException;

	byte[] heartBeat(byte[] serializedHeartBeatRequest) throws RemoteException;

	byte[] jobSubmit(byte[] serializedJobSubmitRequest) throws RemoteException;
}
