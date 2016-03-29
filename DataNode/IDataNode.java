package DataNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.google.protobuf.InvalidProtocolBufferException;

public interface IDataNode extends Remote {
	byte[] readBlock(byte[] serializedReadBlockRequest) throws RemoteException;
	byte[] writeBlock(byte[] serializedWriteBlockRequest) throws RemoteException;
	byte[] heartBeat(byte[] serializedHeartBeatRequest) throws RemoteException, InvalidProtocolBufferException;
}
