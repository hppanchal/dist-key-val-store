package dev;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import structure.Log;
import structure.TCPConnection;

/**
 * This thread is used by the memory server to listen to the get or put requests from the coordinators.
 * It runs in a continuous loop and waits on the coordinator input stream to read the request sent by it.
 * It processes the request and the replies to the request by writing to the output streams.
 * 
 * NOTE: This thread never initializes the sockets or the streams nor does it close them.
 * @author harsh
 *
 */
class receiveMemSvrThread extends Thread {
	
	private TCPConnection coordinatorTCP;
	
	receiveMemSvrThread(TCPConnection coordinatorTCP) {
		this.coordinatorTCP = coordinatorTCP;
	}
	
	public void run() {
		try {
			while(true) {
				Message request;
				Log.write("--------------------------------------------------------------------------------------");
				Log.write("RET(recMemSvrThr): Waiting for request from Co-ord...");
				ObjectInput in;
				in = coordinatorTCP.getInputStream();
				request = (Message)in.readObject();
				Log.write("RET(recMemSvrThr): Request received: "+request);
				
				MessageAck response = request.operate();
				Log.write("RET(recMemSvrThr): Sending response: "+response);
				
				ObjectOutput out;
				out = coordinatorTCP.getOutputStream();
				out.writeObject(response);
				
			} 
		} catch (EOFException e) {
			Log.write("Coordinator failed..! Executing self destruction routine...");
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) { 
			e.printStackTrace();
		}
	}
}

/**
 * This thread is used by the memory server to wait for the coordinator ping in-order to initialize its TCP socket streams to the coordinator
 * On receiving the request, the streams get initialized and these streams are then stored in the coordConn object which is then used by
 * receiveMemSvrThread to listen to coordinator requests.
 * 
 * This thread is started prior to the memSvr sending an IPMessage to the coordinator to register itself.
 * 
 * @author harsh
 *
 */
class establishConnectionMS extends Thread{
	private ServerSocket serversocket;
	private TCPConnection coordConn;
	
	establishConnectionMS(ServerSocket serversocket, TCPConnection coordConn) {
		this.serversocket = serversocket;
		this.coordConn = coordConn;
		this.start();
	}
	
	public void run() {
		try {
			Log.write("RET(estConnMS): Started execConnMS thread..");
			Socket localSocket = serversocket.accept();

			coordConn.setSocket(localSocket);
			
			ObjectInput in = coordConn.getInputStream();
			in = new ObjectInputStream(coordConn.getSocket().getInputStream());
			Message request = (Message) in.readObject();
			coordConn.setInputStream(in);
			
			Log.write("RET(estConnMS): Ping received: "+request);
			MessageAck response = request.operate();
			Log.write("RET(estConnMS): Ping replied: "+response);
			
			ObjectOutput out = coordConn.getOutputStream();
			out = new ObjectOutputStream(coordConn.getSocket().getOutputStream());
			out.writeObject(response);
			coordConn.setOutputStream(out);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}