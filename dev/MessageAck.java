package dev;
import java.io.Serializable;
import java.util.ArrayList;

import structure.KVPair;

/**
 * This is an abstract class denoting the acknowledgment of the message that is received by a server.
 * @author harsh
 *
 */
public abstract class MessageAck implements Serializable{
	private static final long serialVersionUID = 1L;
	static final int SUCCESS = 1;
	static final int FAIL = -1;
	protected int status;
	
	void setStatus(int status) {
		this.status = status;
	}
	
	int getStatus() {
		return status;
	}
	abstract public String toString();
}

/**
 * This is an acknowledgment to the KVMessage. For get and put requests, it contains the following:
 * 		A. For a "get" request:
 * 			1. The status of the request i.e. SUCCESS, FAIL or KEY_NOT_FOUND
 * 			2. The keyvalList containing	(i) 	key whose value was requested
 * 											(ii)	requested value if status is SUCCESS
 * 		B. For a "put" request:
 * 			1. The status of the request i.e. SUCCESS or FAIL
 * 			2. The keyvalList containing	(i)		key(s) that were requested to be put
 * 											(ii)	previous value(s) of that key(s) in-case they was over-written
 * 											(iii)	the previous time-stamp(s) (time-stamp+coordinator's IPaddress) of the key(s) in-case they were over written
 *
 * @author harsh
 *
 */
class KVMessageAck extends MessageAck {
	private static final long serialVersionUID = 1L;
	static final int KEY_NOT_FOUND = 0;
	private ArrayList<KVPair> keyvalList;
	
	KVMessageAck() {
		super();
		keyvalList = new ArrayList<KVPair>();
	}
	ArrayList<KVPair> getKeyvalList() {
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
	public String toString() {
		String text="";
		
		if(status == KEY_NOT_FOUND) {
			text = "KEY NOT FOUND";
		}
		else if(status == FAIL) {
				text = "FAIL";
			}
			else if(status == SUCCESS) {
				text = "SUCCESS\n";		
				for(int i=0;i<keyvalList.size();i++) {
					text += keyvalList.get(i)+"\n";
				}
			
			}
		return text;
	}
}
/**
 * This is the acknowledgment to the IPMessage. It denotes if the message was SUCCESS or FAIL
 * @author harsh
 *
 */
class IPMessageAck extends MessageAck {
	private static final long serialVersionUID = 1L;
	IPMessageAck() {
	}
	
	public String toString() {
		if(status == FAIL) {
			return "FAIL";
		}
		else if(status == SUCCESS) {
			return "SUCCESS";
		}
		return status+"";
	}
}

/**
 * This is the acknowledgment of the ping message that is sent to a server denoting that the ping was successful
 * @author harsh
 *
 */
class PingAck extends MessageAck {
	private static final long serialVersionUID = 1L;

	public String toString() {
		if(status == FAIL) {
			return "ping FAIL";
		}
		else if(status == SUCCESS) {
			return "ping SUCCESS";
		}
		return status+"";
	}	
}

class AEStatusMessageAck extends MessageAck {
	
	private static final long serialVersionUID = 1L;
	
	public String toString() {
		String text = "";
		if(status == SUCCESS) {
			text ="SUCCESS";
		}
		else if(status == FAIL) {
			text ="FAIL";
		}
		else {
			text ="NONE";
		}
		return text;
	}
}