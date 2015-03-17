package structure;

public class IPPort2 {
	private String ipaddress;
	private int writePort;
	private int antiEntropyPort;
	private boolean isUnreachable;
	
	public IPPort2(String ipaddress, int writePort, int antiEntropyPort) {
		super();
		this.ipaddress = ipaddress;
		this.writePort = writePort;
		this.antiEntropyPort = antiEntropyPort;
		this.isUnreachable = false;
	}
	
	public IPPort2(String ipaddress, String writePort, String antiEntropyPort) {
		super();
		this.ipaddress = ipaddress;
		this.writePort = Integer.parseInt(writePort);
		this.antiEntropyPort = Integer.parseInt(antiEntropyPort);
		this.isUnreachable = false;	
	}
	
	public IPPort2() {
		
	}
	
	public IPPort2(IPPort ipp) {
		this.ipaddress = ipp.getIpaddress();
		this.writePort = ipp.getPort();
	}
	public String getIpaddress() {
		return ipaddress;
	}
	public void setIpaddress(String ipaddress) {
		this.ipaddress = ipaddress;
	}
	public int getWritePort() {
		return writePort;
	}
	public void setWritePort(int writePort) {
		this.writePort = writePort;
	}
	public int getAntiEntropyPort() {
		return antiEntropyPort;
	}
	public void setAntiEntropyPort(int antiEntropyPort) {
		this.antiEntropyPort = antiEntropyPort;
	}
	public boolean isUnreachable() {
		return isUnreachable;
	}
	public void setUnreachable(boolean isUnreachable) {
		this.isUnreachable = isUnreachable;
	}
	public String toString() {
		String text = "("+ipaddress+":"+writePort+":"+antiEntropyPort+")";
		return text;
	}	
	public boolean equals(Object obj) {
		IPPort2 IP = (IPPort2) obj;
		return (IP.getIpaddress().equals(this.ipaddress)) && (IP.getWritePort() == this.writePort) && (IP.getAntiEntropyPort() == this.antiEntropyPort);
	}
}
