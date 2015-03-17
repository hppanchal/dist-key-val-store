package dev;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import structure.IPPort;
import structure.KVPair;
import structure.Log;
import structure.TCPConnection;
import structure.ValueTimeStamp;

class MemoryServer {
	
	static TCPConnection userReqCoordConn = null;
	static TCPConnection antiEntCoordConn = null;
	static KVStore keyvalStore = new KVStore();
	static IPPort userReqIP = null;
	static IPPort antiEntIP = null;
	
	public static void initialize(IPPort selfIP, IPPort coordIP, ServerSocket serversocket) {
		MemoryServer.userReqCoordConn = new TCPConnection();
		userReqCoordConn.setRemoteAddress(coordIP);
		MemoryServer.userReqIP= selfIP;
		Log.write("MemSvr(C): userMemSvrIP set to: "+MemoryServer.userReqIP);
		
		ServerSocket aeSocket = null;
		try {
			aeSocket = new ServerSocket(0);
			antiEntIP = new IPPort(MemoryServer.userReqIP.getIpaddress(), aeSocket.getLocalPort());
			MemoryServer.antiEntCoordConn = new TCPConnection();
			antiEntCoordConn.setRemoteAddress(antiEntIP);
			Log.write("MemSvr(C): antiEntMemSvrIP set to: "+MemoryServer.antiEntIP);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		new establishConnectionMS(serversocket,userReqCoordConn);
		new establishConnectionMS(aeSocket, antiEntCoordConn);
		
		IPMessage ipm = new IPMessage(MemoryServer.userReqIP, MemoryServer.antiEntIP);
		MessageAck response = Network.sendOnce(ipm, userReqCoordConn.getRemoteAddress());
		if (response.getStatus() == MessageAck.FAIL) {
			System.exit(0);
		}
		Log.write("MemSvr(C): Sending selfIP to Coord SUCCESS.");
		Network.startReceivingMS(userReqCoordConn);
		Network.startReceivingMS(antiEntCoordConn);
	}

	static MessageAck executeCommand(KVMessage kvm) {
		ArrayList<KVPair> keyvalList = kvm.getKeyValList();
		String command = kvm.getCommand();
		Log.write("MS(exeCmd): KVM: "+kvm);
		
		if (command.equalsIgnoreCase("GET")) {
			String key = kvm.getKeyValList().get(0).getKey();
			String value = keyvalStore.get(key);
			KVMessageAck response = new KVMessageAck();
			
			if (value == null) {
				response.setStatus(KVMessageAck.KEY_NOT_FOUND);
			}
			else {
				response.addKeyVal(new KVPair(key, value));
				response.setStatus(MessageAck.SUCCESS);
			}
			return response;
		}
		else if (command.equalsIgnoreCase("PUT")) {
			KVPair currKV = null;
			String key = "", value = "", coord = "";
			Date ts = null;
			boolean toCache = false;
			
			KVMessageAck response = new KVMessageAck();
			for (int eachKey = 0; eachKey<keyvalList.size(); eachKey++) {
				currKV = keyvalList.get(eachKey);
				key = currKV.getKey();
				value = currKV.getValue();
				ts = currKV.getTimestamp();
				coord = currKV.getCoordinator();
				toCache = !kvm.getCacheStatus();
				
				KVPair kvp = keyvalStore.put(key, value, ts, coord, toCache);
				if (kvp != null) {
					response.addKeyVal(kvp);
				}
			}
			response.setStatus(MessageAck.SUCCESS);
			return response;
		}
		else if(command.equalsIgnoreCase("RECENT")) {
			KVMessageAck response = new KVMessageAck();
			
			ArrayList<KVPair> recentList = keyvalStore.getRecentElement();
			if(recentList != null) {
				response.addKeyVal(recentList);
			}
			response.setStatus(MessageAck.SUCCESS);
			return response;
		}
		else if(command.equalsIgnoreCase("SUMMARY")){
			KVMessageAck response = new KVMessageAck();
			
			 HashMap<String, ValueTimeStamp> KVMap = keyvalStore.getKVMap();
			 ArrayList<KVPair> allKVPairsList = new ArrayList<KVPair>();
			 
			 for(String key:KVMap.keySet()){
				 allKVPairsList.add(new KVPair(key, KVMap.get(key)));
			 }		 
			 
			if(allKVPairsList != null) {
				response.addKeyVal(allKVPairsList);
			}
			response.setStatus(MessageAck.SUCCESS);
			return response;
		}				
		else {
			KVMessageAck response = new KVMessageAck();
			response.setStatus(MessageAck.FAIL);
			return response;
		}
	}
}
