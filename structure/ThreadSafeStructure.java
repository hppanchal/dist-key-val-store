package structure;

public class ThreadSafeStructure<Type> {
	private Type data;
	
	public ThreadSafeStructure() {
	}
	
	public synchronized void setValue(Type _data) {
		this.data = _data;
	}
	public synchronized Type getValue() {
		return data;
	}
	public synchronized boolean conditionalSet(Type _data, Type expected) {
		if (data.equals(expected)) {
			data = _data;
			return true;
		}
		else {
			return false;
		}
	}
}
