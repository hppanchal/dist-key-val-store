package dev;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import structure.GroupMemberList;
import structure.IPPort;
import structure.IPPort2;
import structure.KVPair;
import structure.Log;
import structure.TCPConnection;
import structure.ThreadSafeStructure;

public class Coordinator {
	static IPPort2 selfIP = new IPPort2();
	static int numOfMemSvrs = -1;
	static ServerSocket serversocketWrite;
	static ServerSocket serversocketAntiEntropy;
	
	static final Lock initMSlock = new ReentrantLock();
	static final Condition memSvrSetCV = initMSlock.newCondition();
	
	static ArrayList<TCPConnection> userReqMSConnection = new ArrayList<TCPConnection>();
	static ArrayList<TCPConnection> antiEntMSConnection = new ArrayList<TCPConnection>();
	static ArrayList<Lock> memSvrStreamLock = new ArrayList<Lock>();
	
	static ThreadSafeStructure<Boolean> antiEntropyBit = new ThreadSafeStructure<Boolean>();
	static ThreadSafeStructure<Integer> checksum = new ThreadSafeStructure<Integer>();
	static GroupMemberList localMemberList = new GroupMemberList();
	
	static KVStore recentStore = null;
	static boolean requestedFromMemSvr = false;
	
	static void initialize(int numOfMemSvrs, IPPort selfIP, ServerSocket serversocket) {
		Coordinator.numOfMemSvrs = numOfMemSvrs;
		Coordinator.selfIP = new IPPort2(selfIP);
		Coordinator.serversocketWrite = serversocket;
		Coordinator.antiEntropyBit.setValue(false);
		Coordinator.checksum.setValue(0);
		
		Network.startReceivingCoord(Coordinator.serversocketWrite, numOfMemSvrs);
		initMSlock.lock();
		
		try {
			while(userReqMSConnection.size() < Coordinator.numOfMemSvrs) {
				Log.write("Coord: Waiting for "+numOfMemSvrs+" memSvr connections. Current connections: "+userReqMSConnection.size());
				memSvrSetCV.await();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			Log.write("Coord: "+numOfMemSvrs+" memServers connected.");
			initMSlock.unlock();
		}
		
		SimpleDB.clearMaster();
		
		try {
			serversocketAntiEntropy = new ServerSocket(0);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		Coordinator.selfIP.setAntiEntropyPort(serversocketAntiEntropy.getLocalPort());
		
		SimpleDB.updateMasterList(Coordinator.selfIP);
		
		new GroupMemberUpdateAttemptThread();
		new WriteListenerThread(serversocketWrite);
		new AntiEntropyCoordinatorThread(Coordinator.serversocketAntiEntropy);
//		new AntiEntropyAttemptThread();	//TODO: REMOVE THIS COMMENT TO START THREAD
	}
	
	private static int getHashValue(String key) {
		char [] keyArray = key.toCharArray();
		int total = 0;
		for(int i=0;i<keyArray.length;i++) {
			total+=(int)keyArray[i];
		}
		return (total%(numOfMemSvrs));
	}
	
	private static boolean getAntiEntropyStatus() {
		return antiEntropyBit.getValue();
	}

	private static void addToCheckSum(ArrayList<KVPair> checksumList) {
		int tempChecksum = 0;
		KVPair currKV;
		
		for(int i=0;i<checksumList.size();i++) {
			currKV = checksumList.get(i);
			String currChecksum = currKV.getTimestamp().getTime()+""+currKV.getCoordinator();
			tempChecksum = tempChecksum ^ currChecksum.hashCode();
		}
		
		int oldChecksum, newChecksum;
		boolean success = false;
		while(!success) {
			oldChecksum = checksum.getValue();
			newChecksum = oldChecksum ^ tempChecksum;
			Log.write("Coord(addToChksum): OldChk: "+oldChecksum+" NewChk: "+newChecksum);
			success = checksum.conditionalSet(newChecksum, oldChecksum);	
		}
	}

	private static boolean changeAntiEntropyAtMS(boolean start) {
		AEStatusMessage request = new AEStatusMessage();
		if(start) {
			request.setOperation(AEStatusMessage.START);
		}
		else {
			request.setOperation(AEStatusMessage.STOP);
		}
		AEStatusMessageAck response = null;
		Log.write("Coord(changeAEatMS): Changing status of MS to "+request.getOperation());
		for(int i=0;i<antiEntMSConnection.size();i++) {
			response = (AEStatusMessageAck) Network.sendToStream(antiEntMSConnection.get(i), request);
			if (response.getStatus() == MessageAck.FAIL) {
				return false;
			}
			Log.write("Coord(changeAEatMS): Status changed to "+request.getOperation()+" at MS "+i);
		}
		return true;
	}
	static MessageAck addMemServer(IPPort userReqMemSvrIP, IPPort antiEntMemSvrIP) {
		IPMessageAck response = new IPMessageAck();
		
		initMSlock.lock();
		try {
			if (userReqMSConnection.size() < numOfMemSvrs) {
				TCPConnection userReqMemSvrTCP = new TCPConnection(userReqMemSvrIP);
				Log.write("Coord(addMS): Added memSvr(UR): "+userReqMemSvrIP+" Establishing connection.");
				boolean connection = Network.establishConnectionCoord(userReqMemSvrTCP);
				if (connection == false) {
					response.setStatus(MessageAck.FAIL);
					return response;
				}
				userReqMSConnection.add(userReqMemSvrTCP);
				Log.write("Coord(addMS): userReq Connection successful");
				
				
				TCPConnection antiEntMemSvrTCP = new TCPConnection(antiEntMemSvrIP);
				Log.write("Coord(addMS): Added memSvr(AE): "+antiEntMemSvrIP+" Establishing connection.");
				connection = Network.establishConnectionCoord(antiEntMemSvrTCP);
				if (connection == false) {
					response.setStatus(MessageAck.FAIL);
					return response;
				}
				antiEntMSConnection.add(antiEntMemSvrTCP);
				Log.write("Coord(addMS): antiEnt Connection successful");
				
				Lock tempLock = new ReentrantLock();
				memSvrStreamLock.add(tempLock);
				response.setStatus(MessageAck.SUCCESS);
				memSvrSetCV.signal();
				return response;
			}
			response.setStatus(MessageAck.FAIL);
			return response;
		} finally {
			initMSlock.unlock();
		}
	}
	
	static boolean startAntiEntropy() {
		boolean set = antiEntropyBit.conditionalSet(true, false);
		if(set) {
			recentStore = new KVStore();
			recentStore.startAntiEntropy();
			recentStore.setUseAtMaster();
			changeAntiEntropyAtMS(true);
			requestedFromMemSvr = false;
		}
		return set;
	}
	
	static boolean stopAntiEntropy() {
		boolean reset = antiEntropyBit.conditionalSet(false, true);
		if(reset) {
			recentStore = null;
			changeAntiEntropyAtMS(false);  //TODO catch the return value
		}
		return reset;
	}
	
	static int getChecksum() {
		return checksum.getValue();
	}
	
	static ArrayList<KVPair> getRecentUpdate() {
		if(!requestedFromMemSvr) {
			Log.write("Coord(getRecentUpdate): Requesting MS for their updates..");
			KVMessage request = new KVMessage("RECENT");
			KVMessageAck response = null;
			ArrayList<KVPair> tempList = null;
			
			for(int i=0;i<antiEntMSConnection.size();i++) {
				response = (KVMessageAck) Network.sendToStream(antiEntMSConnection.get(i), request);
				Log.write("Coord(getRecentUpdate): Response from MS "+i+" is "+response);
				tempList = response.getKeyvalList();
				for(int j=0;j<tempList.size();j++) {
					if(tempList.get(j).getKey()!=null) {
						recentStore.put(tempList.get(j), false);
					}
				}
			}
			requestedFromMemSvr = true;
		}
		
		ArrayList<KVPair> recentList = recentStore.getRecentElement();
		recentStore.delete(recentList);
		
		Log.write("Coord(getRecentUpdate): RecentList is: "+recentList);
		if (!recentStore.isPreviousFromCache()) {
			Log.write("Coord(getRecentUpdate): Previous entry taken was NOT from Cache.");
			requestedFromMemSvr = false;
		}
		return recentList;
	}
	
	static boolean antiEntropyPut(ArrayList<KVPair> kvList) {
		HashMap<Integer,KVMessage> KVMap = new HashMap<Integer, KVMessage>();
		int msNum = -1;
		KVPair currKV;
		KVMessage currKVM;
		for(int i=0;i<kvList.size();i++) {
			currKV = kvList.get(i);
			msNum = getHashValue(currKV.getKey());
			if(!KVMap.keySet().contains(msNum)) {
				KVMap.put(msNum, new KVMessage("PUT"));
			}
			currKVM = KVMap.get(msNum);
			currKVM.addKeyVal(currKV);
			KVMap.put(msNum, currKVM);
		}
		
		boolean success = true;
		KVMessageAck tempKVAck;
		if(KVMap.size() > 0) {
			Iterator<Integer> i = KVMap.keySet().iterator();
			while(i.hasNext()) {
				msNum = i.next();
				currKVM = KVMap.get(msNum);
				currKVM.setCacheStatus(true);
				Log.write("Coord(AEPut): Sending AEPut to MS "+msNum+". Request: "+currKVM);
				tempKVAck = (KVMessageAck)Network.sendToStream(antiEntMSConnection.get(msNum), currKVM);
				Log.write("Coord(AEPut): Received response from MS"+msNum+". Response: "+tempKVAck);
				
				currKVM.addKeyVal(tempKVAck.getKeyvalList());
				addToCheckSum(currKVM.getKeyValList());
				success = success & (tempKVAck.getStatus() == MessageAck.SUCCESS);
			}
		}
		return success;
	}

	static boolean writeToBackup(ArrayList<KVPair> KVPairList) {
		ArrayList<IPPort2> memberList = new ArrayList<IPPort2>();
		
		if(!memberList.isEmpty()) {
			KVMessage backupRequest = new KVMessage("BACKUP");
			backupRequest.addKeyVal(KVPairList);
			Collections.shuffle(memberList);
			for(int i=0; i<memberList.size();i++) {
				IPPort randomBackup = new IPPort(memberList.get(i).getIpaddress(), memberList.get(i).getWritePort());
				MessageAck backupResponse = Network.sendOnce(backupRequest, randomBackup);
				if(backupResponse != null) {
					Coordinator.localMemberList.markReachable(memberList.get(i));
					if (backupResponse.getStatus() == MessageAck.SUCCESS) {
						return true;
					}
				}
				else {
					Coordinator.localMemberList.markUnreachable(memberList.get(i));
				}
			}
		}
		return true;
	}
	
	public static MessageAck executeCommand(ArrayList<KVPair> KVPairList, String command, boolean backupWrite) {
		Log.write("Coord(exeCmd): cmd: "+command+" kvList: "+KVPairList);
		
		HashMap<Integer, ArrayList<String>> subToKeyMap = new HashMap<Integer, ArrayList<String>>();
		ArrayList<Integer> lockList = new ArrayList<Integer>();
		int memSvrNum = -1;
		
		boolean success = true;
		boolean notFound = false;
		KVMessageAck response = new KVMessageAck();
		Date currentTimeStamp = new Date();
		
		for(int i=0;i<KVPairList.size();i++) {
			KVPairList.get(i).setCoordinator(selfIP.getIpaddress());
			KVPairList.get(i).setTimestamp(currentTimeStamp);
			
			memSvrNum = getHashValue(KVPairList.get(i).getKey());
			if(!subToKeyMap.containsKey(memSvrNum)) {
				ArrayList<String> temp = new ArrayList<String>();
				subToKeyMap.put(memSvrNum, temp);
			}
			ArrayList<String> temp = subToKeyMap.get(memSvrNum);
			temp.add(KVPairList.get(i).getKey());
			subToKeyMap.put(memSvrNum, temp);
		}
		
		try {
			for(int sub=0;sub<(numOfMemSvrs);sub++) {
				if(subToKeyMap.containsKey(sub)) {
					memSvrStreamLock.get(sub).lock();
					Log.write("Coord(exeCmd): Acquired lock on memSvrStream: "+sub);
					lockList.add(sub);
				}
			}
			
			ArrayList<String> tempKeyList;
			Iterator<Integer> it = subToKeyMap.keySet().iterator();
			
			while(it.hasNext()) {
				KVMessage tempRequest = new KVMessage(command);
				memSvrNum = it.next();
				tempKeyList = subToKeyMap.get(memSvrNum);
				for(int j=0;j<KVPairList.size();j++) {
					if(tempKeyList.contains(KVPairList.get(j).getKey())) {
						tempRequest.addKeyVal(KVPairList.get(j));
					}
				}
				
				if(getAntiEntropyStatus() == true && tempRequest.getCommand().equalsIgnoreCase("PUT")) {
					Log.write("Coord(exeCmd): AntiEntropy ON. Hence caching here.");
					ArrayList<KVPair> toAddKVP = tempRequest.getKeyValList();
					for(int i=0;i<toAddKVP.size();i++) {
						recentStore.put(toAddKVP.get(i),true);
					}
					tempRequest.setCacheStatus(true);
				}
	
				KVMessageAck tempResponse =(KVMessageAck) Network.sendToStream(userReqMSConnection.get(memSvrNum), tempRequest);
				if(command.equalsIgnoreCase("PUT")) {
					tempRequest.addKeyVal(tempResponse.getKeyvalList());
					ArrayList<KVPair> checksumList = tempRequest.getKeyValList();
					addToCheckSum(checksumList);
				}
				response.addKeyVal(tempResponse.getKeyvalList());
				success = success & (tempResponse.getStatus() == MessageAck.SUCCESS);
				if (tempResponse.getStatus() == KVMessageAck.KEY_NOT_FOUND) {
					notFound = true;
				}
			}
			
			if (command.equalsIgnoreCase("PUT") && !backupWrite) { 
				writeToBackup(KVPairList); // TODO: Catch return value and check
			}
			if(success) {
				response.setStatus(MessageAck.SUCCESS);
			}
			else if(notFound){
				response.setStatus(KVMessageAck.KEY_NOT_FOUND);
			}
			else {
				response.setStatus(MessageAck.FAIL);
			}
			
		} finally {
			for(int i=0;i<lockList.size();i++) {
				memSvrStreamLock.get(lockList.get(i)).unlock();
				Log.write("Coord(exeCmd): Released lock on memSvrStream: "+i);
			}
		}
		return response;
	}

	public static void performAntiEntropy() {
		boolean started = startAntiEntropy();
		Log.write("Coord(performAE): startedAE??: "+started);
		if(started) {
			ArrayList<IPPort2> otherMasters = localMemberList.getMemberList();
			Collections.shuffle(otherMasters);
			Log.write("Coord(performAE): List of AE Masters: "+otherMasters);
			new AntiEntropyClientThread(otherMasters);
		}
	}
	
	public static MessageAck getSummary() {
		KVMessage request = new KVMessage("SUMMARY");
		KVMessageAck memSvrResponse = new KVMessageAck();
		
		KVMessageAck response = new KVMessageAck();
		ArrayList<KVPair> currentClusterKVPair = new ArrayList<KVPair>(); 
		
		// May have to lock here -- Not required since we are using it for debugging only.
		
		for(int i=0;i<antiEntMSConnection.size();i++) {
			memSvrResponse = (KVMessageAck) Network.sendToStream(antiEntMSConnection.get(i), request);
			if (memSvrResponse.getStatus() == MessageAck.SUCCESS) {
				currentClusterKVPair.addAll(memSvrResponse.getKeyvalList());
			}
			else {
				// TODO: Handle exception to bring down the cluster
			}
		}
		for(int k=0;k<currentClusterKVPair.size();k++)
		{
			for(int j=0;j<currentClusterKVPair.size()-1;j++)
			{
				if(currentClusterKVPair.get(j).getKey().compareTo(currentClusterKVPair.get(j+1).getKey())>0)
				{	
					KVPair temp=currentClusterKVPair.remove(j);
					KVPair temp2=currentClusterKVPair.remove(j);
					currentClusterKVPair.add(j,temp2);
					currentClusterKVPair.add(j+1,temp);
				}
			}
		}
		
		Log.write("Coordinator : "+currentClusterKVPair);
		response.addKeyVal(currentClusterKVPair);
		response.setStatus(MessageAck.SUCCESS);
		return response;
	}

}
