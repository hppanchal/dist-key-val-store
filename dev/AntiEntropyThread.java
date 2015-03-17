package dev;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import structure.IPPort2;
import structure.KVPair;
import structure.Log;

class AntiEntropyClientThread extends Thread {
	ArrayList<IPPort2> masterList;
	public AntiEntropyClientThread(ArrayList<IPPort2> masterList) {
		this.masterList = masterList;
		this.start();
	}
	
	public void run() {
		Socket clientSoc = null;
		boolean successfulAE = false;
		Log.write("AEClientThread: Client Thread for AE started..");
		for(int coord =0; (coord<masterList.size() && !successfulAE);coord++) {
			try {
				clientSoc = new Socket(masterList.get(coord).getIpaddress(), masterList.get(coord).getAntiEntropyPort());
				
				AntiEntropyInitiate initiateRequest = new AntiEntropyInitiate();
				initiateRequest.setOperation(AntiEntropyInitiate.REQUEST);
				Log.write("AEClientThread: Initiating AE to: "+masterList.get(coord)+" with msg: "+initiateRequest);
				
				ObjectOutput out = new ObjectOutputStream(clientSoc.getOutputStream());
				out.writeObject(initiateRequest);
				
				ObjectInput in = new ObjectInputStream(clientSoc.getInputStream());
				AntiEntropyInitiate initiateResponse = (AntiEntropyInitiate)in.readObject();
				Log.write("AEClientThread: Response received: "+initiateResponse);
				
				if(initiateResponse.getStatus() == AntiEntropyInitiate.ACCEPTED) {
					Coordinator.localMemberList.markReachable(masterList.get(coord));
					
					successfulAE = true;
					Log.write("AEClientThread: AE request successful to: "+masterList.get(coord));
					AntiEntropyWriteMessage request = new AntiEntropyWriteMessage();
					ArrayList<KVPair> kvList = Coordinator.getRecentUpdate();
					
					request.addKeyVal(kvList);
					request.setChecksum(Coordinator.getChecksum());
					Log.write("AECLientThread: WriteRequest: "+request);
					
					out.writeObject(request);
					
					AntiEntropyWriteMessage response = (AntiEntropyWriteMessage)in.readObject();
					Log.write("AECLientThread: WriteResponse: "+response);
					
					while(!response.isStopAntiEntropy()) {
						request = (AntiEntropyWriteMessage)response.operate();
						Log.write("AECLientThread: WriteRequest: "+request);
						out.writeObject(request);
						response = (AntiEntropyWriteMessage)in.readObject();
						Log.write("AECLientThread: WriteResponse: "+response);
					}
					Coordinator.stopAntiEntropy();
					out.close();
					in.close();
					clientSoc.close();
				}
				Coordinator.localMemberList.markReachable(masterList.get(coord));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				Coordinator.localMemberList.markUnreachable(masterList.get(coord));
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		if(!successfulAE){
			Coordinator.stopAntiEntropy();
		}
	}
}

class AntiEntropyCoordinatorThread extends Thread {
	ServerSocket serversocket;
	
	AntiEntropyCoordinatorThread(ServerSocket serversocket) {
		this.serversocket = serversocket;
		this.start();
	}
	
	public void run() {
		while(true) {
			Log.write("AECoordThread: Waiting for AE request from another coord..");
			try {
				Socket socket = serversocket.accept();
				ObjectInput in = new ObjectInputStream(socket.getInputStream());
				AntiEntropyInitiate initiateRequest = (AntiEntropyInitiate)in.readObject();
				Log.write("AECoordThread: Request for AE received: "+initiateRequest);
				
				AntiEntropyInitiate initiateReponse = (AntiEntropyInitiate)initiateRequest.operate();
				Log.write("AECoordThread: Response to the AE request: "+initiateReponse);
				
				ObjectOutput out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(initiateReponse);
				
				if(initiateReponse.getStatus() == AntiEntropyInitiate.ACCEPTED) {
					Log.write("AECoordThread: AE request ACCEPTED.");
					AntiEntropyWriteMessage request =(AntiEntropyWriteMessage) in.readObject();
					Log.write("AECoordThread: WriteRequest: "+request);
					
					AntiEntropyWriteMessage response = (AntiEntropyWriteMessage)request.operate();
					Log.write("AECoordThread: WriteResponse: "+response);
					out.writeObject(response);
					
					while(!response.isStopAntiEntropy()) {
						request = (AntiEntropyWriteMessage)in.readObject();
						Log.write("AECoordThread: WriteRequest: "+request);
						response = (AntiEntropyWriteMessage) request.operate();
						Log.write("AECoordThread: WriteResponse: "+response);
						out.writeObject(response);
					}					
					Coordinator.stopAntiEntropy();
				}
				out.close();
				in.close();
				socket.close();
				
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}