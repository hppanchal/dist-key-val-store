package structure;

/**
 * This is the doubly linked list that is used to store the most recent updates in the Hash-Map of the KVStore
 * 
 * @author harsh
 *
 */
public class DoublyLinkedList {
   private Link first;          
   private Link last;           
   private Link antiEntropyPointer;

   public DoublyLinkedList() {
      first = null;             
      last = null;
      antiEntropyPointer = null;
   }

   public boolean isEmpty() {
	   return first==null;
   }

   public void initializePointer(){
	   antiEntropyPointer = first;
   }
   
   public boolean listTopGreaterEqual(String timestamp) {
	   if(isEmpty())
		   return false;
	   else if(antiEntropyPointer == null){
		   return false;
	   }
	   else {
		   String currTimestamp = antiEntropyPointer.timeStamp;
		   if (currTimestamp.compareToIgnoreCase(timestamp) >= 0) {
			   return true;
		   }
		   else {
			   return false;
		   }
	   }
   }
   
   public String getNext() {
	   if(isEmpty())
		   return null;
	   else if(antiEntropyPointer == null){
		   return null;
	   }
	   else {
		   String returnValue =  antiEntropyPointer.key;
		   antiEntropyPointer = antiEntropyPointer.next;
		   return returnValue;
	   }
   }
   
   public String toString() {
	   String list="";
	   Link current=first;
	   while(current!=null) {
		   list+=current.key+" ";
		   current=current.next;
	   }
	   return list;
   }
   
   private void insertFirst(String key, String timeStamp) {
      Link newLink = new Link(key,timeStamp);
      if(isEmpty()) {
    	   last = newLink;  
      }
      else {
    	  first.previous = newLink; 
      }
      newLink.next = first;          
      first = newLink;
    }

   public void insertUsingTS(String key, String timeStamp) {
	   if(isEmpty()) {
		   insertFirst(key,timeStamp); 
	   }
	   else {
		   Link current = first;
		   Link prev =null;
		   while(current!=null && (current.timeStamp).compareTo(timeStamp)>0) {
			  prev = current;
		      current = current.next;
		   }
		   Link newLink = new Link(key,timeStamp);
		   
		   if(current ==null) {
			   prev.next = newLink;
			   newLink.previous = prev;
			   last = newLink;
		   }
		   else if(current==first) {
			   insertFirst(key,timeStamp); 
		   }
		   else {
			   prev.next = newLink;
			   newLink.next = current;			   
			   newLink.previous = prev;
			   current.previous = newLink;
	      }
		   return;
	   }
   }

   
   public Link deleteKey(String key) {                             
      Link current = first;         
      while(current.key != key) {
         current = current.next;   
         if(current == null) {
        	 return null; 
         }
      }
      if(current==first) {
    	 first = current.next;  
      }
            
      else {
    	  current.previous.next = current.next; 
      }
      
      if(current==last) {
    	  last = current.previous;
      }
      else {                           
    	  current.next.previous = current.previous; 
      }
      return current;                
   }
}

class Link {
	String key;                 
	Link next;                
	Link previous;            
	String timeStamp;
	
	Link(String key,String timeStamp) {
		this.key = key;
		this.timeStamp=timeStamp;}
	
	void displayLink() {
		System.out.print("["+key + " " + timeStamp+"] -> "); 
	}
}