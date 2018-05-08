package edu.upenn.cis455.mapreduce.master;

import edu.upenn.cis.stormlite.Config;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  private MasterServletInfo msi = new MasterServletInfo();
  private final String lineSep = System.getProperty("line.separator");
  
  public void doPost(HttpServletRequest request, HttpServletResponse response) 
	       throws java.io.IOException{
	  try {		
		  String uri = request.getRequestURI();
		  // handle submitjob
		  switch(uri){
		  	case "/submitjob":
		  		handleSubmitJob(request, response);
		  		break;
		  	default:
		  		response.sendError(500, "Command not supported");
		  		break;
		  }
	  } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	  }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) 
       throws java.io.IOException
  {
	  try {		
		String uri = request.getRequestURI();

		// handle workerstatus, status and shutdown
		
		switch(uri){
	  	case "/workerstatus":
	  		System.out.println("Receiving worker status");
			handleWorkerStatus(request, response);
			return;
	  	case "/status":
	  		System.out.println("Showing worker status");
			showStatus(request, response);
			return;
	  	case "/shutdown":
	  		System.out.println("Shutting down");
	  		handleShutdown(request, response);
	  		return;
	  	default:
	  		response.sendError(500, "Command not supported");
	  		break;
	  }
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    response.setContentType("text/html");
    PrintWriter out = response.getWriter();
    out.println("<html><head><title>Master</title></head>");
    out.println("<body>Hi, I am the master!</body></html>");
  }
  
  private void handleShutdown(HttpServletRequest request, HttpServletResponse response) 
		  throws IOException{
	  //shutdown master and workers
	  msi.sendShutdown();
  }
  
  // submit job to workers
  private void handleSubmitJob(HttpServletRequest request, HttpServletResponse response) 
		  throws IOException{
	  System.out.println(request.getRequestURL());
	  System.out.println(request.getQueryString());
	  System.out.println(request.toString());
	  // read input
	  String seedurl = request.getParameter("seedurl");
	  String maxSize = request.getParameter("maxSize");
	  String numFile = request.getParameter("numFile");
	  
	  System.out.println(seedurl);
	  System.out.println(maxSize);
	  System.out.println(numFile);
	  
	  Config config = new Config();
	  config.put("seedurl", seedurl);
	  config.put("job", seedurl);
	  // try convert
	  try {
		  Integer.parseInt(maxSize);
		  Integer.parseInt(numFile);
	  } catch (NumberFormatException e){
		  System.out.println(e);
		  System.exit(1);
	  }
      
//      // IP:port for /workerstatus to be sent
//      config.put("jobName", jobClassName);
//      // Class with map function
//      config.put("mapClass", jobClassName);
//      // Class with reduce function
//      config.put("reduceClass", jobClassName);
      
      // Numbers of executors (per node)

	  config.put("maxSize", maxSize);
	  config.put("numFile", numFile);

	  // submit job
	  msi.submitCrawlerJob(config);
  }
  
  // show status of each worker
  private void showStatus(HttpServletRequest request, HttpServletResponse response) 
		  throws IOException{
	  response.setContentType("text/html");
	  PrintWriter pw = response.getWriter();
	  
	  StringBuilder page = new StringBuilder();
	  page.append("<html>").append(lineSep);
	  page.append("<head>").append(lineSep);

//	  page.append("Check Status and Launch Jobs").append(lineSep);
	  page.append("Yecheng Yang (yecheng)").append(lineSep);
	  page.append("</head>").append(lineSep);
	  page.append("<body>").append(lineSep);
	  page.append("<table>").append(lineSep);
		
	  
	  page.append("<tr><td>Active workers in the system: </td></tr>").append(lineSep);
	  page.append("<tr><td>IP:port</td><td>Status</td><td>Job Name</td><td>Keys Read</td><td>Keys Written</td></tr>").append(lineSep);
	  for (Map.Entry<String, WorkerStatus> entry: msi.getActiveWorkers().entrySet()){
		  String ipAddr = entry.getKey();
		  WorkerStatus ws = entry.getValue();
		  page.append("<tr>").append(lineSep);
		  page.append("<td>" + ipAddr + "</td>").append(lineSep);
		  page.append("<td>" + ws.getStatus()+ "</td>").append(lineSep);
//		  page.append("<td>" + ws.getJobName()+ "</td>").append(lineSep);
		  page.append("<td>" + ws.getKeysRead()+ "</td>").append(lineSep);
//		  page.append("<td>" + ws.getKeysWritten()+ "</td>").append(lineSep);
		  page.append("</tr>").append(lineSep);
	  }
	  page.append("</table>").append(lineSep);
	  page.append("<form method=\"post\" action=\"/submitjob\">").append(lineSep);
	  page.append("Seed Url: <input type=\"text\" name=\"seedurl\"><br/>").append(lineSep);
	  page.append("Max file size: <input type=\"number\" name=\"maxSize\"><br/>").append(lineSep);
	  page.append("Number files: <input type=\"number\" name=\"numFile\"><br/>").append(lineSep);

	  page.append("<input type=\"submit\" value=\"Sumbut Job\"><br/>").append(lineSep);
	  page.append("</form>").append(lineSep);
	  page.append("</body>").append(lineSep);
	  page.append("</html>").append(lineSep);
	  pw.print(page.toString());
	  pw.close();
  }
  
  // receive status update from worker
  private void handleWorkerStatus(HttpServletRequest request, HttpServletResponse response) 
		  throws IOException{
	
	  // get all parameters
	  String port = request.getParameter("port");
	  String status = request.getParameter("status");
//	  String jobName = request.getParameter("job");
	  String keysRead = request.getParameter("keysRead");
//	  String keysWritten = request.getParameter("keysWritten");
//	  String resultsStr = request.getParameter("results");
	  String ipAddr = request.getRemoteAddr()+":"+port;
	  
	  System.out.println(ipAddr);
	  System.out.println(port);
	  System.out.println(status);

	  
	  // decode all parameters
	  port = decodeUTF(port);
	  status = decodeUTF(status);
//	  jobName = decodeUTF(jobName);
	  keysRead = decodeUTF(keysRead);
//	  keysWritten = decodeUTF(keysWritten);
//	  resultsStr = decodeUTF(resultsStr);
	  
//	  String[] results = ArrayHelper.convertToArray(resultsStr);
	  
	  // add status
//	  msi.putStatus(ipAddr, new WorkerStatus(port, status, jobName, keysRead, keysWritten, results, ipAddr));
      msi.putStatus(ipAddr, new WorkerStatus(port, status, ipAddr, keysRead));
  }
  
  private String decodeUTF(String input) throws UnsupportedEncodingException{
	  return URLDecoder.decode(input, "UTF-8").trim();
  }
}


  
