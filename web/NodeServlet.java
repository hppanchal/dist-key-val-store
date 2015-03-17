package web;

import dev.*;

import java.io.IOException;
import java.util.ArrayList;
import structure.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class NodeServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static boolean isCoord = false;
	
    public NodeServlet() {
        super();
        Log.startLog();
        isCoord = Node.initialize(2);
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if (isCoord) {
			if (request.getParameter("cmd")!=null) {
				String command = request.getParameter("cmd");
				ArrayList<KVPair> kvList = new ArrayList<KVPair>();
				Log.write("NodeServlet: Request received: "+command);
				
				if(command.equals("GET")) {
					String key = request.getParameter("NewText");
					KVPair tempKV = new KVPair();
					tempKV.setKey(key);
					kvList.add(tempKV);
					
					Log.write("NodeServlet: Processed KVPairs: "+kvList);
					MessageAck coordResponse= Coordinator.executeCommand(kvList, command, false);
					request.setAttribute("requestStatus",coordResponse); //TODO make this string
					request.getRequestDispatcher("index.jsp").forward(request, response);
				}
				else if(command.equals("SUMMARY")){
					MessageAck coordResponse= Coordinator.getSummary();
					Log.write("NodeServlet : "+coordResponse);
					String responseOnScreen = coordResponse.toString().replace("\n", "<br/>");	
					response.setContentType("text/plain");
				    response.setCharacterEncoding("UTF-8");
				    response.getWriter().write(responseOnScreen);
					
				}else if(command.equals("AntiEntropy")){
					Coordinator.performAntiEntropy();
					response.sendRedirect("index.jsp");
				}
			}
			else {
				response.sendRedirect("index.jsp");
			}
		}
		else {
			return;
		}
		Log.write("--------------------------------------------------------------------------------------");
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		if (isCoord) {
			if (request.getParameter("cmd")!=null) {
				String command = request.getParameter("cmd");
				ArrayList<KVPair> kvList = new ArrayList<KVPair>();
				Log.write("NodeServlet: Request received: "+command);
				
				if(command.equals("PUT")) {
					String putRequest = request.getParameter("NewText");
					putRequest = putRequest.trim();
					String [] eachPut = putRequest.split(";");
					
					for(int i=0;i<eachPut.length;i++) {
						String [] keyval = eachPut[i].split(":");
						KVPair tempKV = new KVPair(keyval[0],keyval[1]);
						kvList.add(tempKV);
					}
					
					Log.write("NodeServlet: Processed KVPairs: "+kvList);
					MessageAck coordResponse = Coordinator.executeCommand(kvList, command, false);
					request.setAttribute("requestStatus",coordResponse);
					request.getRequestDispatcher("index.jsp").forward(request, response);
					
				}
			}
			else{
				response.sendRedirect("index.jsp");
			}
		}
		else{
		return;
		}
		Log.write("--------------------------------------------------------------------------------------");
	}
	

}
