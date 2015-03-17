package dev;

import java.io.Serializable;
import java.util.ArrayList;

import structure.IPPort;
import structure.KVPair;
import structure.Log;

public abstract class AntiEntropyMessage implements Serializable {
	private static final long serialVersionUID = 1L;
	abstract AntiEntropyMessage operate();
	abstract public String toString();
}

class AntiEntropyInitiate extends AntiEntropyMessage{
	static final int REQUEST = 1;
	static final int RESPONSE = 2;
	static final int ACCEPTED = 3;
	static final int REJECTED = -1;
	
	private static final long serialVersionUID = 1L;
	private int operation;
	private int status = -2;
	private IPPort coordIP;
	
	public AntiEntropyInitiate(IPPort coordIP, int status) {
		this.coordIP = coordIP;
		this.status = status;
	}
	public AntiEntropyInitiate () {
		
	}
	public int getOperation() {
		return operation;
	}
	public void setOperation(int operation) {
		this.operation = operation;
	}
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public IPPort getCoordIP() {
		return coordIP;
	}

	public void setCoordIP(IPPort coordIP) {
		this.coordIP = coordIP;
	}

	public AntiEntropyMessage operate() {
		AntiEntropyInitiate response = new AntiEntropyInitiate();
		response.setOperation(RESPONSE);
		
		if(operation == REQUEST) {
			boolean started = Coordinator.startAntiEntropy();
			if (started) {
				response.setStatus(ACCEPTED);
			}
			else {
				response.setStatus(REJECTED);
			}
		}
		else {
			response.setStatus(REJECTED);
		}
		return response;
	}
	
	public String toString() {
		String opr = "";
		if(operation == REQUEST) {
			opr = "REQUEST";
		}
		else if(operation == RESPONSE) {
			opr = "RESPONSE";
		}
		else {
			opr = "INVALID OPERATION";
		}
		String st = "";
		if(status == ACCEPTED) {
			st = "ACCEPTED";
		}
		else if(status == REJECTED) {
			st = "REJECTED";
		}
		else {
			st = "NONE";
		}
		return "(Operation: "+opr+" Status: "+st+" coordIP: "+coordIP+")";
	}
}

class AntiEntropyWriteMessage extends AntiEntropyMessage {
	private static final long serialVersionUID = 1L;
	private ArrayList<KVPair> keyvalList;
	private int checksum;
	private boolean stopAntiEntropy = false;
	
	public AntiEntropyWriteMessage() {
		keyvalList = new ArrayList<KVPair>();
	}
	public ArrayList<KVPair> getKeyvalList() {
		return keyvalList;
	}
	void addKeyVal(ArrayList<KVPair> keyvalList) {
 		for(int i=0;i<keyvalList.size();i++) {
 			this.keyvalList.add(keyvalList.get(i));
 		}
 	}
 	void addKeyVal(KVPair keyval) {
 		this.keyvalList.add(keyval);
 	}

	public int getChecksum() {
		return checksum;
	}

	public void setChecksum(int checksum) {
		this.checksum = checksum;
	}

	public boolean isStopAntiEntropy() {
		return stopAntiEntropy;
	}

	public void setStopAntiEntropy(boolean stopAntiEntropy) {
		this.stopAntiEntropy = stopAntiEntropy;
	}

	AntiEntropyMessage operate() {
		AntiEntropyWriteMessage response = new AntiEntropyWriteMessage();
		
		if(stopAntiEntropy) {
			Log.write("AEWriteMsg(operate): Request wanted to stopAntiEntropy.");
			response.setStopAntiEntropy(true);
			return response;
		}
		else {
			Coordinator.antiEntropyPut(keyvalList); //TODO check if this is successful
			
			int currChecksum = Coordinator.getChecksum();
			if(currChecksum == checksum) {
				Log.write("AEWriteMsg(operate): Both checksums match. Stopping AE.");
				response.setStopAntiEntropy(true);
				return response;
			}
			
			ArrayList<KVPair> recentList = Coordinator.getRecentUpdate();
			Log.write("AECoordThread: RecentList: "+recentList);
			if(recentList!=null) {
				response.addKeyVal(recentList);
			}
			response.setChecksum(currChecksum);
			return response;
		}
	}
	
	public String toString() {
		String text = "Checksum: "+checksum+" Stop: "+stopAntiEntropy+"\n";
		text+= "KVList: "+keyvalList;
		return text;
	}	
}