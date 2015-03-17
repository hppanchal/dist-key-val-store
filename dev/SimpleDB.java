package dev;

import java.util.ArrayList;
import java.util.List;

import structure.IPPort;
import structure.IPPort2;
import structure.Log;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.UpdateCondition;

public class SimpleDB {

	private static ArrayList<IPPort2> getListFromStr(String strList) {
		String [] strIPList = strList.split("_");
		String [] eachIP;
		IPPort2 tempIP;
		ArrayList<IPPort2> IPList = new ArrayList<IPPort2>();
		
		for(int i=0;i<strIPList.length;i++) {
			eachIP = strIPList[i].split(":");
			tempIP = new IPPort2(eachIP[0], eachIP[1], eachIP[2]);
			
			IPList.add(tempIP);
		}
		return IPList;
	}
	
	private static String getStrFromList(ArrayList<IPPort2> memberList) {
		StringBuilder newMemberList = new StringBuilder();
		IPPort2 currIP;
		for(int i=0;i<memberList.size();i++) {
			currIP = memberList.get(i);
			newMemberList.append(currIP.getIpaddress()+":"+currIP.getWritePort()+":"+currIP.getAntiEntropyPort());
			if(i < (memberList.size()-1)) {
				newMemberList.append("_");
			}
		}
		return newMemberList.toString();
	}

	static IPPort checkForMaster(IPPort selfIP) {
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());
		String cannonicalIP = selfIP.getIpaddress()+"_"+selfIP.getPort();
		
		UpdateCondition expected=new UpdateCondition("address", "NONE", true);
		List<ReplaceableAttribute> attrList = new ArrayList<ReplaceableAttribute>();
		attrList.add(new ReplaceableAttribute("address",cannonicalIP,true));
		
		try {
			sdb.putAttributes(new PutAttributesRequest("CSMENGDOMAIN","Coordinator", attrList, expected));
			Log.write("Node(checkSimpleDB): Became Co-ordinator");
			return selfIP;
		}
		catch (AmazonServiceException ase) {
			if(ase.getStatusCode() == 409) {
				String selectExpression = "select * from CSMENGDOMAIN";
		        SelectRequest selectRequest = new SelectRequest(selectExpression);
		        List<Item> item = sdb.select(selectRequest).getItems();
		        
		        List<Attribute> attr = item.get(0).getAttributes();
		        
		        for (Attribute currAttr: attr ) {
		        	if(currAttr.getName().equals("address")) {
		        		cannonicalIP = currAttr.getValue();
		        		String [] IPP = cannonicalIP.split("_");
		        		IPPort newCoord;
		        		if(IPP.length == 2) {
		        			newCoord= new IPPort (IPP[0], IPP[1]);
		        			Log.write("Node(checkSimpleDB): Failed to become Co-ordinator. Co-ordIP is: "+newCoord);
		        			return newCoord;
		        		}
		        	}
		        }
			}
			return null;
		}
	}
	
	static void clearMaster() {
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());	
		List<ReplaceableAttribute> attrList = new ArrayList<ReplaceableAttribute>();
		attrList.add(new ReplaceableAttribute("address","NONE",true));
		sdb.putAttributes(new PutAttributesRequest("CSMENGDOMAIN","Coordinator", attrList));
		Log.write("Coord: Cleared simpleDB for reuse");
	}
	
	/*
	static boolean addToMasterList(IPPort selfIP) {
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());
		String selectExpression = "select * from CSMENGDOMAIN";
        SelectRequest selectRequest = new SelectRequest(selectExpression);
        List<Item> item = sdb.select(selectRequest).getItems();
        List<Attribute> attr = item.get(0).getAttributes();
        
        String listOfMasters = "";
        
        for (Attribute currAttr: attr ) {
        	if(currAttr.getName().equals("list")) {
        		listOfMasters = currAttr.getValue();
        		break;
        	}
        }
		
        String newListOfMasters = "";
        if(!listOfMasters.equals("NONE")) {
        	ArrayList<IPPort> masterList = getListOfIP(listOfMasters);
            if (masterList.contains(selfIP)) {
            	return true;
            }
            newListOfMasters = listOfMasters+"_"+selfIP.getIpaddress()+":"+selfIP.getPort();
        }
        else {
        	newListOfMasters = selfIP.getIpaddress()+":"+selfIP.getPort();
        }
        
		UpdateCondition expected=new UpdateCondition("list", listOfMasters, true);
		List<ReplaceableAttribute> attrList = new ArrayList<ReplaceableAttribute>();
		attrList.add(new ReplaceableAttribute("list",newListOfMasters,true));
		
		try {
			sdb.putAttributes(new PutAttributesRequest("CSMENGDOMAIN","Coordinator", attrList, expected));
			return true;
		}
		catch (AmazonServiceException ase) {
			if(ase.getStatusCode() == 409) {
				
			}
			return addToMasterList(selfIP);
		}
	}
	
	static ArrayList<IPPort> getListOfMasters(IPPort selfIP) {
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());
		String selectExpression = "select * from CSMENGDOMAIN";
        SelectRequest selectRequest = new SelectRequest(selectExpression);
        List<Item> item = sdb.select(selectRequest).getItems();
        List<Attribute> attr = item.get(0).getAttributes();
        
        String listOfMasters = "";
        
        for (Attribute currAttr: attr ) {
        	if(currAttr.getName().equals("list")) {
        		listOfMasters = currAttr.getValue();
        		break;
        	}
        }
        ArrayList<IPPort> masterList;
        if(listOfMasters.equals("NONE")) {
        	masterList = new ArrayList<IPPort>();
        }
        else {
        	masterList = getListOfIP(listOfMasters);
        	if(masterList.contains(selfIP)) {
        		masterList.remove(selfIP);
        	}
        }
        return masterList;
	}
	
	*/
	static boolean updateMasterList(IPPort2 selfIP) {
		AmazonSimpleDB sdb = new AmazonSimpleDBClient(new ClasspathPropertiesFileCredentialsProvider());
		String selectExpression = "select * from CSMENGDOMAIN";
        SelectRequest selectRequest = new SelectRequest(selectExpression);
        List<Item> item = sdb.select(selectRequest).getItems();
        List<Attribute> attr = item.get(0).getAttributes();
        
        String listOfMasters = "";
        
        for (Attribute currAttr: attr ) {
        	if(currAttr.getName().equals("list")) {
        		listOfMasters = currAttr.getValue();
        		break;
        	}
        }
        ArrayList<IPPort2> masterList = new ArrayList<IPPort2>();
        String newListOfMasters = "";
        if(!listOfMasters.equals("NONE")) {
        	masterList = getListFromStr(listOfMasters);
            ArrayList<IPPort2> localList = Coordinator.localMemberList.getMemberList();
            
            for(int i=0;i<localList.size();i++) {
            	if(localList.get(i).isUnreachable()) {
            		masterList.remove(localList.get(i));
            	}
            	else if (!masterList.contains(localList.get(i))){
            		masterList.add(localList.get(i));
            	}
            }
            
            if (!masterList.contains(selfIP)) {
            	masterList.add(selfIP);
            }
            
            newListOfMasters = getStrFromList(masterList);
            
        }
        else {
        	if(!Coordinator.localMemberList.isListEmpty()) {
                ArrayList<IPPort2> localList = Coordinator.localMemberList.getMemberList();
        		for(int i=0;i<localList.size();i++) {
        			if (!localList.get(i).isUnreachable()){
                		masterList.add(localList.get(i));
                	}
                }
        	}
        	masterList.add(selfIP);
        	newListOfMasters = getStrFromList(masterList);
        }
        
		UpdateCondition expected=new UpdateCondition("list", listOfMasters, true);
		List<ReplaceableAttribute> attrList = new ArrayList<ReplaceableAttribute>();
		attrList.add(new ReplaceableAttribute("list",newListOfMasters,true));
		
		try {
			sdb.putAttributes(new PutAttributesRequest("CSMENGDOMAIN","Coordinator", attrList, expected));
			masterList.remove(selfIP);
			Coordinator.localMemberList.setMemberList(masterList);
			return true;
		}
		catch (AmazonServiceException ase) {
			if(ase.getStatusCode() == 409) {
				return updateMasterList(selfIP);				
			}
			else {
				return false;
			}
		}
	}
}
