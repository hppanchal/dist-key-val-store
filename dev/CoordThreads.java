package dev;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import structure.Log;

/**
 * This thread is started by the ListenerThread which is listening on the coordinator for memSvr registration requests.
 * This thread is given the socket of the connection as soon as a request is received.
 * It reads the request message, processes it and send a reply to the request
 * 
 * @author harsh
 *
 */
 class RequestExecutionThead extends Thread {
	private Socket serverSoc;
	
	RequestExecutionThead(Socket socket) {
		this.serverSoc = socket;
	}
	
	public void run() {	
		Message request;
		ObjectInput in;
		ObjectOutput out;
		
		try {
			in = new ObjectInputStream(serverSoc.getInputStream());
			request = (Message)in.readObject();
			Log.write("RET: Request received: "+request);
			
			MessageAck response = request.operate();
			
			out = new ObjectOutputStream(serverSoc.getOutputStream());
			out.writeObject(response);
			Log.write("RET: Response sent: "+response);
			
			in.close();
			out.close();
			serverSoc.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}

/**
 * This is a thread that is used by the coordinator to listen to memSvr registration requests.
 * As soon as a request arrives and a socket is created, this thread spawns an instance of the ReceiveExecutionThread and
 * gives the socket to this thread for reading the request, processing it and replying to the message
 * 
 * After a given number of connections, this thread quits. The number of connections is typically the number of memSvrs the coordinator is
 * expecting to get registered to it
 * 
 * @author harsh
 *
 */
class ListenerThread extends Thread {
	
	private ServerSocket serversocket;
	private int numOfConnections;
	
	ListenerThread(ServerSocket serversocket, int numOfConnections) {
		this.serversocket = serversocket;
		this.numOfConnections = numOfConnections;
	}
	
	public void run() {
		Socket serverSoc = null;
		int currentConn = 0;
		
		while (currentConn < numOfConnections) {
			try {
				serverSoc = serversocket.accept();
				RequestExecutionThead ret = new RequestExecutionThead(serverSoc);
				ret.start();
				currentConn++;
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

class AntiEntropyAttemptThread extends Thread {
	public AntiEntropyAttemptThread() {
		this.start();
	}
	public void run() {
		
		try {
			while(true) {
				Coordinator.performAntiEntropy();
				Thread.sleep(200); // TODO: Give this a variance
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

class WriteListenerThread extends Thread {
	private ServerSocket serversocket;
	public WriteListenerThread(ServerSocket serversocket) {
		this.serversocket = serversocket;
		this.start();
	}
	public void run() {
		Socket serverSoc = null;
		
		try {
			while(true){
				serverSoc = serversocket.accept();
				
				Message request;
				ObjectInput in;
				ObjectOutput out;
				
				in = new ObjectInputStream(serverSoc.getInputStream());
				request = (Message)in.readObject();
				Log.write("WLT: Request received: "+request);
				
				MessageAck response = request.operate();
				
				out = new ObjectOutputStream(serverSoc.getOutputStream());
				out.writeObject(response);
				Log.write("WLT: Response sent: "+response);
				
				in.close();
				out.close();
				serverSoc.close();
				
			} 
		}catch (IOException e) {
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}	

class GroupMemberUpdateAttemptThread extends Thread {
	public GroupMemberUpdateAttemptThread() {
		this.start();
	}
	public void run() {
		Random random = new Random();
		
		try {
			while(true) {
				Thread.sleep(30000);	// TODO: Change time here..
				int max = Coordinator.localMemberList.getNumOfMembers();
				if (max > 0) {
					int randomNum = random.nextInt(max);
					if(randomNum == max/2) {
						SimpleDB.updateMasterList(Coordinator.selfIP);
					}
				}
				else {
					SimpleDB.updateMasterList(Coordinator.selfIP);
				}
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}