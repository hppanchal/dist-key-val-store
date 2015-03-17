package structure;

import java.util.ArrayList;

public class GroupMemberList {
	private ArrayList<IPPort2> memberList;
	
	public GroupMemberList() {
		memberList = new ArrayList<IPPort2>();
	}
	public synchronized ArrayList<IPPort2> getMemberList() {
		return new ArrayList<IPPort2> (memberList);
	}
	public synchronized void setMemberList(ArrayList<IPPort2> memberList) {
		this.memberList = memberList;
	}
	public synchronized boolean isListEmpty() {
		return (memberList.size() == 0);
	}
	public synchronized int getNumOfMembers() {
		return memberList.size();
	}
	public synchronized void markUnreachable(IPPort2 location) {
		for(int i=0;i<memberList.size();i++) {
			if(memberList.get(i) == location) {
				memberList.get(i).setUnreachable(true);
				break;
			}
		}
	}
	public synchronized void markReachable(IPPort2 location) {
		for(int i=0;i<memberList.size();i++) {
			if(memberList.get(i) == location) {
				memberList.get(i).setUnreachable(false);
				break;
			}
		}
	}
}
