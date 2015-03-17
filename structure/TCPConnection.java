package structure;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.Socket;

/**
 * This class is used to store the socket and the open streams between the memory server and the coordinator
 * @author harsh
 *
 */
public class TCPConnection {
	private IPPort remoteAddress;
	private Socket localSocket;
	private ObjectOutput out;
	private ObjectInput in;
	
	public TCPConnection() {
		this.remoteAddress = null;
		this.localSocket = null;
		this.out = null;
		this.in = null;
	}
	
	public TCPConnection(IPPort remoteAddress) {
		this.remoteAddress = remoteAddress;
		try {
			localSocket = new Socket(remoteAddress.getIpaddress(), remoteAddress.getPort());
			in = null;
			out = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public ObjectOutput getOutputStream() {
		return out;
	}
	public void setOutputStream(ObjectOutput out) {
		this.out = out;
	}
	public ObjectInput getInputStream(){
		return in;
	}
	public void setInputStream(ObjectInput in) {
		this.in = in;
	}
	public Socket getSocket() {
		return localSocket;
	}
	public void setSocket(Socket localSocket) {
		this.localSocket = localSocket;
	}
	public IPPort getRemoteAddress() {
		return remoteAddress;
	}
	public void setRemoteAddress(IPPort remoteAddress) {
		this.remoteAddress = remoteAddress;
	}
	public boolean connectionEstablished() {
		return (in!=null && out!=null);
	}
	
	protected void finalize() {
		try {
			out.close();
			in.close();
			localSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
