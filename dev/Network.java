package dev;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import structure.IPPort;
import structure.Log;
import structure.TCPConnection;

/**
 * This class contains all the network layer functions used for sending and receiving messages across servers
 * @author harsh
 *
 */
public class Network {
	/**
	 * This function is used to send a message once to a destination. It typically opens a socket, sends a message across to the
	 * destination and then closes the socket and the associated streams after the reply of the message is received.
	 * 
	 * @param message		The message to be sent 
	 * @param destination	The IP port to whom the message has to be sent
	 * @return response		The acknowledgment MessageAck of the sent message
	 */
	static MessageAck sendOnce(Message message, IPPort destination) {
		Log.write("Network(sendOnce): Message: "+message+" destination: "+destination);
		try {
			Socket socket = new Socket(destination.getIpaddress(), destination.getPort());
			ObjectOutput out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(message);
			
			ObjectInput in = new ObjectInputStream(socket.getInputStream());
			MessageAck response = (MessageAck)in.readObject();
			Log.write("Network(sendOnce): response received: "+response);
			
			out.close();
			in.close();
			socket.close();
			return response;
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	/**
	 * This function is used to send a message to an already initialized message stream.
	 * Such a stream exists between the memory server and the coordinator.
	 * This function just writes to the output stream and then reads the reply from the input stream.
	 * NOTE: It does not initialize the streams nor does it close the streams and the sockets
	 * 
	 * @param tcp		The object containing the streams of to which the message has to be written and read from
	 * @param message	The message to be sent
	 * @return response	The acknowledgment MessageAck of the sent message
	 */
	static MessageAck sendToStream(TCPConnection tcp, Message message) {
		Log.write("Network(sendToStream): Message: "+message+" destination: "+tcp.getRemoteAddress());
		MessageAck response = null;
		try {
			ObjectOutput out = tcp.getOutputStream();
			out.writeObject(message);
			
			ObjectInput in = tcp.getInputStream();
			response = (MessageAck)in.readObject();
			Log.write("Network(sendToStream): response received: "+response);
			
		} catch (IOException e) {
			Log.write("Memory Server: "+tcp.getRemoteAddress()+" is down. Shutting down..");
			System.exit(0);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return response;
	}
	
	/**
	 * This function starts a thread that is used by the coordinator to listen to register requests from the memory servers
	 * during the initialization stage.
	 * 
	 * @param serversocket		The ServerSocket on which the coordinator accepts requests 
	 * @param numOfConnections	The total number of requests this thread should receive before exiting
	 */
	static void startReceivingCoord(ServerSocket serversocket, int numOfConnections) {
		ListenerThread lt = new ListenerThread(serversocket, numOfConnections);
		lt.start();
	}
	
	/**
	 * This function starts a thread that is used by the memory servers to listen to the coordinator requests.
	 * Is reads the input stream of the coordinator connection and writes a response to the output stream of the same.
	 * 
	 * NOTE: It does not initialize streams or sockets nor does it close a stream or socket.
	 * 
	 * @param tcp			which connects the socket and the streams of the coordinator
	 */
	static void startReceivingMS(TCPConnection tcp) {
		receiveMemSvrThread rot = new receiveMemSvrThread(tcp);
		rot.start();
	}
	
	/**
	 * This function is used by the coordinator to send a ping message to the memory server in-order to initialize its streams.
	 * This is typically executed after the coordinator receives a memory server registration request (IPMessage) and before it sends it an
	 * acknowledgment.
	 * 
	 * @param MSConn	The object that has the socket and the streams of the connection between the memSvr and the coordinator
	 * @return			A boolean indicating if the initialization was successful or not
	 */
	static boolean establishConnectionCoord(TCPConnection MSConn) {
		Ping ping = new Ping();
		try {
			ObjectOutput out = MSConn.getOutputStream();
			out = new ObjectOutputStream(MSConn.getSocket().getOutputStream());
			out.writeObject(ping);
			Log.write("Network(estConnCoord) ping sent:"+ping);
			MSConn.setOutputStream(out);
			
			ObjectInput in = MSConn.getInputStream();
			in = new ObjectInputStream(MSConn.getSocket().getInputStream());
			MessageAck response = (MessageAck) in.readObject();
			Log.write("Network(estConnCoord) ping received: "+response);
			MSConn.setInputStream(in);
			
			if(response.getStatus() == MessageAck.SUCCESS) {
				return true;
			}
			else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return false;
		}
	}
}
