package structure;
import java.util.Date;
/**
 * An object of this class is stored as the 'value' of the key-value store.
 * This is a wrapper class on the value of the key, and the time-stamp which includes
 * 						(i) the time at which the client request reached the coordinator
 * 						(ii) the IPaddress of the coordinator that sent the request
 * 
 * @author harsh
 */

public class ValueTimeStamp {
	private String Value;
	private String CoordinatorIP;
	private Date timeStamp;	
	
	public ValueTimeStamp(){
		this.Value = "";
		this.CoordinatorIP = "";
		this.timeStamp = new Date();
	}
	
	public ValueTimeStamp(String value, Date timeStamp,String coordinatorIP) {
		super();
		this.Value = value;
		this.CoordinatorIP = coordinatorIP;
		this.timeStamp = timeStamp;
	}
	
	
	public ValueTimeStamp(ValueTimeStamp duplicate){
		this.Value = duplicate.Value;
		this.CoordinatorIP = duplicate.CoordinatorIP;
		this.timeStamp = duplicate.timeStamp;
	}
	
	public String getValue() {
		return Value;
	}

	public void setValue(String value) {
		this.Value = value;
	}

	public String getCoordinatorIP() {
		return CoordinatorIP;
	}

	public void setCoordinatorIP(String coordinatorIP) {
		this.CoordinatorIP = coordinatorIP;
	}

	public Date getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public String toString(){
		return "["+this.Value+", "+this.timeStamp+", "+this.CoordinatorIP+"]";
	}
}
