package structure;
import java.io.Serializable;

/**
 * This class is a wrapper class for storing the IP and the port numbers of the coordinator and the memory servers
 * @author harsh
 *
 */
public class IPPort implements Serializable
{
	private static final long serialVersionUID = 1L;
	private String ipaddress;
	private int port;
	
	public IPPort () {
		
	}
	public IPPort(String ipaddress, int port) {
		super();
		this.ipaddress = ipaddress;
		this.port = port;
	}
	public IPPort(String ipaddress, String port) {
		super();
		this.ipaddress = ipaddress;
		this.port = Integer.parseInt(port);
	}
	public IPPort(IPPort ip) {
		this.ipaddress = ip.getIpaddress();
		this.port = ip.getPort();
	}
	
	public String getIpaddress() {
		return ipaddress;
	}
	public void setIpaddress(String ipaddress) {
		this.ipaddress = ipaddress;
	}
	public int getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = Integer.parseInt(port);
	}
	public void setPort(int port) {
		this.port = port;
	}
	
	public String toString() {
		String text = "("+ipaddress+":"+port+")";
		return text;
	}
	
	public boolean equals(Object obj) {
		IPPort IP = (IPPort) obj;
		return (IP.getIpaddress().equals(this.ipaddress)) && (IP.getPort() == this.port);
	}
}
