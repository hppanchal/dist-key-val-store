package dev;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;

import structure.IPPort;
import structure.Log;


public class Node {
	
	static IPPort coordIP;
	static int numOfMachines = 0; 
	static ServerSocket serversocket = null;
	static IPPort selfIP = new IPPort();
	
	public static boolean initialize(int numOfMachines) {
		Node.numOfMachines = numOfMachines;
		setSelfIP();
		
		try {
			serversocket = new ServerSocket(0);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		selfIP.setPort(serversocket.getLocalPort());
			Log.write("Node(C): selfIP: "+selfIP);
		
		coordIP = SimpleDB.checkForMaster(selfIP);
		
		if(selfIP.equals(coordIP)) {
			Coordinator.initialize((numOfMachines-1), selfIP, serversocket);
			return true;
		}
		else {
			MemoryServer.initialize(selfIP, coordIP, serversocket);
			return false;
		}
	}
	
	 
	
	private static void setSelfIP() {
		Enumeration<NetworkInterface> nets;
		try {
			nets = NetworkInterface.getNetworkInterfaces();
			for (NetworkInterface netint : Collections.list(nets)) {
				Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
		
				for (InetAddress inetAddress : Collections.list(inetAddresses)) {
					String s = inetAddress+"";
					if (inetAddress instanceof Inet4Address &&  !s.contains("127.0.")) {
						selfIP.setIpaddress(inetAddress.getHostAddress());
					}
				}
			}
		} catch (SocketException e) {
			e.printStackTrace();
	  }
	}
}
