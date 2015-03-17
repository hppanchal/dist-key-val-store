package dev;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import structure.DoublyLinkedList;
import structure.KVPair;
import structure.ValueTimeStamp;
/**
 * This class is the actual structure used to store the key-value-pairs. It resides in the memory server.
 * It has the following 3 structures:
 * 				1. A Hash-Map that is used to store the key with the value & time -stamp
 * 				2. A doubly linked list that is used to maintain the keys in the order of the most recent insertion
 * 				3. A checksum that is calculated over all the (keys + time-stamps) currently in the Hash-Map
 * 
 * @author harsh
 *
 */
class KVStore {
	HashMap<String, ValueTimeStamp> KVMap = new HashMap<String, ValueTimeStamp>();
	DoublyLinkedList recentKeyList =  new DoublyLinkedList();
	ArrayList <KVPair> cache;
	boolean antiEntropy = false;
	int checksum = 0;
	boolean previousFromCache = false;
	boolean useAtMaster = false;
	
	
	/**
	 * This function is used to retrieve a key from the Hash-Map. If the key is not present it the map, it returns a null
	 *  
	 * @param key	The key whose value is requested
	 * @return		The value of the key if it exists or else null
	 */
	synchronized String get(String key){
		if(KVMap.containsKey(key)) {
			return KVMap.get(key).getValue();
		}
		else {
			return null;
		}
	}
	

	synchronized void delete(ArrayList<KVPair> kvList) {
		String key;
		for(int i=0;(kvList!=null && i<kvList.size());i++) {
			key = kvList.get(i).getKey();
			if(KVMap.containsKey(key)) {
				KVMap.remove(key);
				recentKeyList.deleteKey(key);
			}
		}
	}
	
	/**
	 * This is a private function that is used to put the key along with the value (ValueTimeStamp) into the Hash-Map.
	 * It returns the old time-stamp (ValueTimeStamp) in-case it already has a key present, else it returns null
	 * The key is adjusted in the doubly linked-list (if already present) or inserted into the list (if not previously present)
	 * 
	 * @param key		The key to be stored
	 * @param vts		The value along with the time-stamp to be store against the key
	 * @return			A KVPair object that has the time-stamps of the previous entry of the key
	 */
	synchronized private KVPair put(String key,ValueTimeStamp vts, boolean toCache) {	
		if(KVMap.keySet().contains(key)){
			ValueTimeStamp oldVTS = KVMap.get(key);
			if(vts.getTimeStamp().before(oldVTS.getTimeStamp())) {
				return new KVPair(key, vts);
			}
			else {
				computeChecksum(key,oldVTS);
				KVMap.put(key, new ValueTimeStamp(vts));		
				recentKeyList.deleteKey(key);
				recentKeyList.insertUsingTS(key,vts.getTimeStamp().getTime()+""+vts.getCoordinatorIP());
				computeChecksum(key,vts);
				if(antiEntropy && toCache) {
					KVPair kvp = new KVPair(key, vts);
					cache.add(kvp);
				}
				return new KVPair(key,oldVTS);
			}			
		}
		else {
			KVMap.put(key, new ValueTimeStamp(vts));			
			recentKeyList.insertUsingTS(key,vts.getTimeStamp().getTime()+""+vts.getCoordinatorIP());
			computeChecksum(key,vts);
			
			if(antiEntropy && toCache) {
				KVPair kvp = new KVPair(key, vts.getValue(), vts.getTimeStamp(), vts.getCoordinatorIP());
				cache.add(kvp);
			}
			return null;
		}
	}
	
	/**
	 * This is a helper method that calls the put method that takes a key and a ValueTimeStamp object
	 * 
	 * @param key				Key to be inserted 
	 * @param value				Value of the key
	 * @param timeStamp			Time-stamp at which the request was made
	 * @param coordinatorIP		The IP of the coordinator who serviced the request
	 * @return					A KVPair object that has the time-stamps of the previous entry of the key
	 */
	synchronized KVPair put(String key,String value,Date timeStamp, String coordinatorIP, boolean toCache){
		ValueTimeStamp newTuple = new ValueTimeStamp(value,timeStamp, coordinatorIP);
		return put(key,newTuple, toCache);
	}
	
	synchronized KVPair put(KVPair kvp ,boolean toCache){
		ValueTimeStamp newTuple = new ValueTimeStamp(kvp.getValue(), kvp.getTimestamp(), kvp.getCoordinator());
		return put(kvp.getKey(),newTuple, toCache);
	}
	/**
	 * This is used to set the antiEnropy bit if it is not set
	 * 
	 * @return	True after setting the bit if it not already set else False 
	 */
	synchronized boolean startAntiEntropy(){
		if(!antiEntropy) {
			antiEntropy = true;
			recentKeyList.initializePointer();
			cache = new ArrayList<KVPair>();
			return true;
		}
		else {
			return false;
		}
	}
	/**
	 * This is used to reset the antiEntropy bit if it is set
	 * 
	 * @return True after resetting the bit if it is already set else False
	 */
	synchronized boolean stopAntiEntropy(){
		if(antiEntropy){
			antiEntropy = false;
			cache = null;
			return true;
		}
		else {
			return false;
		}
	}
	/**
	 * This function is used to compute the checksum on the key and time-stamp of a given key-value pair.
	 * The function also adds the computed checksum to its master-checksum that is calculated over all the values in the Hash-Map
	 * 
	 * @param key
	 * @param value		This is a ValueTimeStamp object
	 */
	synchronized void computeChecksum(String key, ValueTimeStamp value) {
		String hashable = key+""+value.getTimeStamp().getTime()+value.getCoordinatorIP()+"";
		int currentHashVal = hashable.hashCode();
		checksum = checksum ^ currentHashVal;
	}
	
	/**
	 * This function returns the next recently modified key in the Hash-Map while performing anti-Entropy
	 * 
	 * @return		A KVPair that has the next recent update in the Hash-Map that is pointed by the antiEntropyPointer
	 */
	synchronized ArrayList<KVPair> getRecentElement() {
		if(useAtMaster) {
			recentKeyList.initializePointer();
		}
		
		if(antiEntropy) {
			ArrayList<KVPair> recentList = new ArrayList<KVPair>();
			
			if(recentKeyList.isEmpty() && cache.size() == 0) {
				return recentList;
			}
			else {
				if(cache.size() > 0) {
					KVPair cacheTop;
					cacheTop = cache.get(cache.size()-1);
					
					String timestamp = cacheTop.getTimestamp().getTime()+""+cacheTop.getCoordinator();
					boolean listGreater = recentKeyList.listTopGreaterEqual(timestamp);
					
					if (!listGreater) {
						cache.remove(cacheTop);
						recentList.add(cacheTop);
						previousFromCache = true;
						
						String prevTimestamp = timestamp;
						while(cache.size() > 0) {
							cacheTop = cache.get(cache.size()-1);
							timestamp = cacheTop.getTimestamp().getTime()+""+cacheTop.getCoordinator();
							if(timestamp.equals(prevTimestamp)) {
								cache.remove(cacheTop);
								recentList.add(cacheTop);
							}
							else {
								break;
							}
						}
						return recentList;
					}
				}
				
				String key;
				ValueTimeStamp vts;
				
				key = recentKeyList.getNext();
				if (key != null) {
					vts = KVMap.get(key);
				}
				else {
					vts = new ValueTimeStamp();
				}
				
				KVPair mostRecentKVP = new KVPair(key,vts);
				recentList.add(mostRecentKVP);
				previousFromCache = false;
				
				String timestamp = vts.getTimeStamp().getTime()+""+vts.getCoordinatorIP();
				boolean addMore = recentKeyList.listTopGreaterEqual(timestamp);
				
				while(addMore) {
					key = recentKeyList.getNext();
					vts = KVMap.get(key);
					mostRecentKVP = new KVPair(key,vts);
					recentList.add(mostRecentKVP);
					addMore = recentKeyList.listTopGreaterEqual(timestamp);
				}
				return recentList;
			} 
		}
		else {
			return null;
		}
	}
	
	synchronized boolean getAntiEntropyStatus() {
		return antiEntropy;
	}
	
	synchronized boolean isPreviousFromCache() {
		return previousFromCache;
	}
	synchronized void setUseAtMaster() {
		useAtMaster = true;
	}


	HashMap<String, ValueTimeStamp> getKVMap() {
		return KVMap;
	}
}