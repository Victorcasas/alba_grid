package com.imcs.grid.taskcontroller.commands.commons;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.FileInfo;

public class SendURLToHost extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.info("******************************************************");
		logger.info("[PROCESSING] Init SendURLToHost " + parameters);
		logger.info("******************************************************");
		
		long timeStart = System.currentTimeMillis();
		FileInfo[] files = (FileInfo[])action.getRequest().get("files");
		logger.info(files.length + " files received like param");
		String urlHost = (String)parameters.getProperty("url.host");
		try {
			String jcl = (String)action.getRequest().get("jcl");
			URL url = null;
			URLConnection  conn = null;
			BufferedReader in = null;
			String inputLine = null;
			for (int i=0,iHasta=files.length;i<iHasta;i++) {		
				logger.info("SendURLToHost::" + urlHost + " " + jcl + " " + files[i].getName());
				url = new URL(urlHost + " " + jcl + " " + files[i].getName());
				conn = url.openConnection();
				conn.connect();
				in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				
		        while ((inputLine = in.readLine()) != null) 
		            logger.info(inputLine.getBytes());
		        in.close();		
			}
		}
		catch (Throwable th) {
			logger.error("Error sending url to Host",th);
		}
		logger.info("******************************************************");
		logger.info("[PROCESSING] End SendURLToHost");
		logger.logTime("[PROCESSING] SendURLToHost Time ", timeStart ,System.currentTimeMillis());
		logger.info("******************************************************");
		
		return action.findForward("success");			
	}
	
	public SendURLToHost() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void checkInput(Action action) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}