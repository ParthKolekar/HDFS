package TaskTracker;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskTracker extends UnicastRemoteObject {

	class MapTask implements Runnable {

		private Integer blockID;

		public MapTask(Integer blockID) {
			this.blockID = blockID;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}

	class ReduceTask implements Runnable {

		private Integer blockID;

		public ReduceTask(Integer blockID) {
			this.blockID = blockID;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub

		}

	}

	private static final long serialVersionUID = 1L;
	private static final String configurationFile = "Resources/tasktracker.properties";
	private static Integer exitTimeout;

	private static String networkInterface;

	private static ThreadPoolExecutor executor;

	public static void main(String[] args) throws IOException {

		Properties properties = new Properties();
		InputStream inputStream = new FileInputStream(configurationFile);
		properties.load(inputStream);

		networkInterface = properties.getProperty("Network Interface");
		exitTimeout = Integer.parseInt(properties.getProperty("Exit Timeout"));

		if ((networkInterface == null) || (exitTimeout == null)) {
			System.out.println("Configuration Missing...");
			System.exit(-1);
		}

		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			@Override
			public void run() {
				executor.shutdown();
				try {
					executor.awaitTermination(exitTimeout, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					System.out.println("Task Inturrupted...");
					System.exit(-1);
				}
			}
		}));

		Inet4Address inetAddress = null;
		try {
			Enumeration<InetAddress> enumeration = NetworkInterface.getByName(networkInterface).getInetAddresses();
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

		System.setProperty("java.rmi.server.hostname", inetAddress.getHostAddress());
		LocateRegistry.getRegistry(inetAddress.getHostAddress(), Registry.REGISTRY_PORT).rebind("TaskTracker", new TaskTracker());

		System.out.println("Loaded TaskTracker...");
	}

	public TaskTracker() throws RemoteException {
		super();
	}

}
