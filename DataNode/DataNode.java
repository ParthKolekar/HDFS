package DataNode;

import java.io.File;
import java.io.FileFilter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Enumeration;

import Protobuf.HDFSProtobuf.BlockLocations;
import Protobuf.HDFSProtobuf.ReadBlockRequest;
import Protobuf.HDFSProtobuf.ReadBlockResponse;
import Protobuf.HDFSProtobuf.WriteBlockRequest;
import Protobuf.HDFSProtobuf.WriteBlockResponse;

import com.google.protobuf.ByteString;

public class DataNode extends UnicastRemoteObject implements IDataNode {

	private static final String networkInterface = "eth0";
	private static final long serialVersionUID = 1L;
	private static final Integer heartBeatTimeout = 1000;
	private static Integer heartBeatID = 0;
	private static String dataDirectory = "/hdfs";

	public static void main(String[] args) throws RemoteException,
			MalformedURLException, AlreadyBoundException {

		// new Thread(new Runnable() {
		//
		// @Override
		// public void run() {
		// while (true) {
		// heartBeatID++;
		// HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest
		// .newBuilder();
		// heartBeatRequest.setId(heartBeatID);
		// byte[] serializedHeartBeatRequest = heartBeatRequest
		// .build().toByteArray();
		//
		// INameNode nameNode = null;
		//
		// try {
		// nameNode = (INameNode) Naming.lookup("NameNode");
		// } catch (MalformedURLException | RemoteException
		// | NotBoundException e) {
		// e.printStackTrace();
		// }
		//
		// byte[] serializedHeartBeatResponse = null;
		//
		// try {
		// serializedHeartBeatResponse = nameNode
		// .heartBeat(serializedHeartBeatRequest);
		// } catch (RemoteException | InvalidProtocolBufferException e) {
		// e.printStackTrace();
		// }
		//
		// HeartBeatResponse heartBeatResponse = null;
		//
		// try {
		// heartBeatResponse = HeartBeatResponse
		// .parseFrom(serializedHeartBeatResponse);
		// } catch (InvalidProtocolBufferException e) {
		// e.printStackTrace();
		// }
		//
		// Integer heartBeatStatus = heartBeatResponse.getStatus();
		//
		// if (heartBeatStatus.equals(heartBeatID)) {
		// System.out.println(" Heart Beating... ");
		// }
		//
		// try {
		// Thread.sleep(heartBeatTimeout);
		// } catch (InterruptedException e) {
		// // nope
		// }
		// }
		// }
		// }).start();

		new Thread(new Runnable() {

			@Override
			public void run() {

				File directoryFile = new File(dataDirectory);
				File[] blockNumbers = directoryFile.listFiles(new FileFilter() {

					@Override
					public boolean accept(File pathname) {
						return pathname.isFile() && !pathname.isHidden()
								&& pathname.canRead()
								&& pathname.getName().matches("^-?\\d+$");
					}
				});

				if (blockNumbers == null) {
					System.err.println("Error Reading Data Directory");
					System.exit(-1);
				}

				Inet4Address inetAddress = null;

				try {
					Enumeration<InetAddress> enumeration = NetworkInterface
							.getByName(networkInterface).getInetAddresses();
					while (enumeration.hasMoreElements()) {
						InetAddress tempInetAddress = enumeration.nextElement();
						if (tempInetAddress instanceof Inet4Address) {
							inetAddress = (Inet4Address) tempInetAddress;
						}
					}
				} catch (SocketException e) {
					e.printStackTrace();
				}
				if (inetAddress == null) {
					System.err.println("Error Obtaining Network Information");
					System.exit(-1);
				}

				System.out.println(Registry.REGISTRY_PORT);
				System.out.println(inetAddress.getHostAddress());

				for (File tempFile : blockNumbers) {
					System.out.println(tempFile.getName());
				}
			}
		}).start();

		// try {
		// dataNode.check();
		// Path path = Paths.get(Integer.toString(1));
		// byte[] data = Files.readAllBytes(path);
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// System.out.println(e);
		// }

		// DataNode dataNode = new DataNode();
		// Naming.rebind("DataNode", dataNode);
	}

	// public void check() throws RemoteException,
	// InvalidProtocolBufferException
	// {
	// ReadBlockRequest.Builder readBlockRequest =
	// ReadBlockRequest.newBuilder();
	// readBlockRequest.setBlockNumber(1);
	//
	// byte[] serializedReadBlockRequest =
	// readBlockRequest.build().toByteArray();
	// // DataNode a = new DataNode();
	// byte [] response = this.readBlock(serializedReadBlockRequest);
	// ReadBlockResponse readBlockResponse =
	// ReadBlockResponse.parseFrom(response);
	// System.out.println(readBlockResponse.getStatus());
	//
	// }

	public DataNode() throws RemoteException {
		super();
	}

	@Override
	public byte[] readBlock(byte[] serializedReadBlockRequest)
			throws RemoteException {
		try {
			ReadBlockRequest readBlockRequest = ReadBlockRequest
					.parseFrom(serializedReadBlockRequest);
			Integer blockNumber = readBlockRequest.getBlockNumber();
			Path path = Paths.get(Integer.toString(blockNumber));
			byte[] data = Files.readAllBytes(path);
			ReadBlockResponse.Builder readBlockResponse = ReadBlockResponse
					.newBuilder();
			readBlockResponse.addData(ByteString.copyFrom(data));
			readBlockResponse.setStatus(1);
			return readBlockResponse.build().toByteArray();
		} catch (Exception e) {
			return ReadBlockResponse.newBuilder().setStatus(0).build()
					.toByteArray();
		}
	}

	@Override
	public byte[] writeBlock(byte[] serializedWriteBlockRequest)
			throws RemoteException {
		try {

			WriteBlockRequest writeBlockRequest = WriteBlockRequest
					.parseFrom(serializedWriteBlockRequest);
			ByteString data = ByteString.copyFrom(writeBlockRequest
					.getDataList());
			BlockLocations blockLocations = writeBlockRequest.getBlockInfo();
			Integer blockNumber = blockLocations.getBlockNumber();
			Path path = Paths.get(Integer.toString(blockNumber)); // TODO: Check
																	// for file
																	// creation
			Files.write(path, data.toByteArray());
			WriteBlockResponse.Builder writeBlockResponse = WriteBlockResponse
					.newBuilder();
			writeBlockResponse.setStatus(1);
			return writeBlockResponse.build().toByteArray();
		} catch (Exception e) {
			return (WriteBlockResponse.newBuilder().setStatus(0)).build()
					.toByteArray();
		}
	}

}
