package structure;

import java.io.Serializable;
import java.util.Date;
/**
 * This class denotes a key value pair along with its time-stamp i.e. (time-stamp+coordinator's IPaddress)
 * @author harsh
 *
 */
public class KVPair implements Serializable {
	private static final long serialVersionUID = 1L;
	private String key = "";
	private String value = "";
	private Date timestamp = null;
	private String coordinatorIP = "";
	
	public KVPair(){
		
	}
	public KVPair(String key, String value, Date timestamp, String coordinatorIP) {
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
		this.coordinatorIP = coordinatorIP;
	}
	public KVPair(String key, String value) {
		this.key = key;
		this.value = value;
		this.timestamp = null;
		this.coordinatorIP = null;
	}
	public KVPair(String key, ValueTimeStamp vts) {
		this.key = key;
		this.value = vts.getValue();
		this.timestamp = vts.getTimeStamp();
		this.coordinatorIP = vts.getCoordinatorIP();
	}
	
	public KVPair(KVPair kv) {
		this.key = kv.getKey();
		this.value = kv.getValue();
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	public String getCoordinator() {
		return coordinatorIP;
	}
	public void setCoordinator(String coordinator) {
		this.coordinatorIP = coordinator;
	}
	public String toString() {
		String text = "Key: "+key+" Value: "+value;
		if(timestamp!=null) {
			text+=" Timestamp: "+timestamp;
		}
		if(coordinatorIP!=null) {
			text+=" CoordIP: "+coordinatorIP;
		}
		return text;
	}
}
