package dev;
import java.io.Serializable;
import java.util.ArrayList;

import structure.IPPort;
import structure.KVPair;
import structure.Log;

/**
 * This is an abstract class used to encapsulate the messages that are sent over the network.
 * @author harsh
 *
 */

abstract class Message implements Serializable {
	private static final long serialVersionUID = 1L;
	
	Message () {
	}
	abstract MessageAck operate();
	public abstract String toString();
}

/**
 * This message is used by the memory server to register itself to the coordinator.
 * It contains the IPaddress of the memory server and the port number at which it is listening to coordinator requests
 * @author harsh
 *
 */

 class IPMessage extends Message{
	 private static final long serialVersionUID = 1L;
	 private IPPort userReqIP;
	 private IPPort antiEntIP;
	
	IPMessage(IPPort userReqIP, IPPort antiEntIP) {
		super ();
		this.userReqIP = new IPPort(userReqIP);
		this.antiEntIP = new IPPort(antiEntIP);
	}
	
	void setUserReqIP(IPPort userReqIP) {
		this.userReqIP = userReqIP;
	}
	 
	IPPort getUserReqIP() {
		return userReqIP;
	}
	public IPPort getAntiEntIP() {
		return antiEntIP;
	}

	public void setAntiEntIP(IPPort antiEntIP) {
		this.antiEntIP = antiEntIP;
	}

	/**
	 * This is an overloaded function. In case of an IPMessage arriving at a server, this function would be called by the receiver.
	 * This message will typically be received by the coordinator, hence, the addSMemServer() method of the coordinator class is called
	 */
	MessageAck operate() {
		MessageAck response;
		response = Coordinator.addMemServer(userReqIP, antiEntIP);
		return response;
	}

	public String toString() {
		String text = "userReqIP: "+userReqIP+" antiEntIP: "+antiEntIP;
		return text;
	}
 }
 
 /**
  * This message is used by the coordinator to send a client put or get request to the memory servers.
  * This message contains the command (i.e. put or get) and a list of key value pairs on whom the command has to be executed.
  * @author harsh
  *
  */
class KVMessage extends Message {
	 private static final long serialVersionUID = 1L;
	 private String command = "";
 	 private ArrayList<KVPair> keyvalList;
 	 private boolean cachedAtCoord = false;
 	 
 	KVMessage (String command) {
 		 super();
 		 this.command = command;
 		 this.keyvalList = new ArrayList<KVPair>();
 	 }
 	 
 	String getCommand() {
 		return command;
 	}

 	void setCommand(String command) {
 		this.command = command;
 	}

	void addKeyVal(ArrayList<KVPair> keyvalList) {
 		for(int i=0;i<keyvalList.size();i++) {
 			this.keyvalList.add(keyvalList.get(i));
 		}
 	}
 	
 	void addKeyVal(KVPair keyval) {
 		this.keyvalList.add(keyval);
 	}
 	ArrayList<KVPair> getKeyValList() {
 		return keyvalList;
 	}
 	
 	boolean getCacheStatus() {
 		return cachedAtCoord;
 	}
 	void setCacheStatus(boolean status) {
 		cachedAtCoord = status;
 	}
 	/**
 	 * This is an overloaded function. It will be called every time a memory server receives a KVMessage. This function forwards the message
 	 * to the executeCommand() function of the memory server.
 	 */
 	MessageAck operate() {
 		MessageAck response;
 		if (command.equalsIgnoreCase("BACKUP")) {
 			response = Coordinator.executeCommand(keyvalList, "PUT", true);
 		}
 		else {
 			response = MemoryServer.executeCommand(this);
 		}
 		 return response;
 	 }
 	 
 	 public String toString() {
 		 String text =" Command: "+command+" Cached: "+cachedAtCoord+"\n";
 		 text+="KeyValueList: \n";
 		 for(int i=0;i<keyvalList.size();i++) {
 			 text+=keyvalList.get(i)+"\n";
 		 }
 		 return text;
 	 }
 }

 /**
  * This is a simple ping message used to check the network connection between the server and the coordinator
  * This message is used to initialize the socket and the streams between the memory server and the coordinator when 
  * the memory server registers itself to the coordinator for the first time (using an IPMessage).
  * @author harsh
  *
  */
 class Ping extends Message {
	private static final long serialVersionUID = 1L;

	MessageAck operate() {
		PingAck responsePing = new PingAck();
		responsePing.setStatus(MessageAck.SUCCESS);
		return responsePing;
	}
	public String toString() {
		return "ping! request";
	}
 }
 
 class  AEStatusMessage extends Message {
		private static final long serialVersionUID = 1L;
		
		static final int START = 1;
		static final int STOP = -1;
		
		private int operation;

		public int getOperation() {
			return operation;
		}
		public void setOperation(int change) {
			this.operation = change;
		}
		public MessageAck operate() {
			Log.write("AEStatusMsg(operate): Initial Request to: "+this);
			
			boolean done;
			if(operation == START) {
				done = MemoryServer.keyvalStore.startAntiEntropy();
			}
			else {
				done = MemoryServer.keyvalStore.stopAntiEntropy();
			}
			
			AEStatusMessageAck response = new AEStatusMessageAck();
			
			if(done) {	
				response.setStatus(MessageAck.SUCCESS);
			}
			else {
				response.setStatus(MessageAck.FAIL);
			}
			Log.write("AEStatusMsg(operate): Response to request: "+response);
			return response;
		}
		
		public String toString() {
			String text = (operation == START)? "START":"STOP";
			return text;
		}
	}