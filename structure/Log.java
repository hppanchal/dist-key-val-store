package structure;

public class Log {
	private static boolean logStatus = false;
	
	public static void startLog(){
		logStatus = true;
	}
	public static void stopLog() {
		logStatus = false;
	}
	
	public static void write(String text) {
		if(logStatus == true) {
			System.out.println(text);
		}
	}
}
