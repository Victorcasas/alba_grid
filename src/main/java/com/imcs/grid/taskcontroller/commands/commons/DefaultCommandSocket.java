package com.imcs.grid.taskcontroller.commands.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.UtilString;

public class DefaultCommandSocket extends TaskControllerCommand 
{
	

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException
	{
		long initTime = System.currentTimeMillis();
	
		final StringBuilder sb = new StringBuilder();
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		String ipNode = Node.getMyself().getIp();
		
		int exitCode = 0;
		
		final String pid = (String)action.getRequest().get("pid");
		
		/* Generic executions parameters */
		String idService = (String)action.getRequest().get("idService");
		logger.info("ID SERVICE :: " + idService);
		try {			
			logger.info("****************************************************************");
			logger.info("* [PROCESSING] Init DEFAULT COMMAND SOCKET FROM SERVICE :: " + idService);
			logger.info("****************************************************************");
			

			sb.append("Service "+idService+" init at "+new Date()+"\n");
			
			/* Get XML message parameters */
			String xmlParam = (String) action.getRequest().get("parameters");
			sb.append("Parameters: "+xmlParam+"\n");
			/* Directory where you run the command */
			String workingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(workingDir)) logger.warn("fc-working-dir is null or empty.");
			logger.info("Execution working dir :: " + workingDir);
						
			/* Get the socket port */
			int scktPort = Integer.parseInt(parameters.getProperty("port"));		
				
			
			sb.append("Creating new socket for host: "+ipNode+":"+scktPort);
			Socket echoSocket = new Socket(ipNode, scktPort);
			
			PrintWriter out = new PrintWriter(echoSocket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
			
			out.println(xmlParam);
			String socketOutput = in.readLine();
								
			String[] listOutput = socketOutput.split(";;;");
			exitCode = Integer.parseInt(listOutput[0]);
			exitStatusCode = listOutput[1];
			final String logCodePath = listOutput[2];
			exitCodeDescription = ((listOutput.length > 3)?listOutput[3]:"");
			sb.append("Server response: " +socketOutput.replace(";;;", " ")+"\n");
			out.close();
			echoSocket.close();
			
			logger.info(" exitCode : " + exitCode );
			logger.info(" exitStatus : " + exitStatusCode);
			logger.info(" path : " + logCodePath);
			logger.info(" exitDesc : " + exitCodeDescription);
			
			logger.info("****************************************************************");
			logger.info("* [PROCESSING] End DEFAULT COMMAND SOCKET FROM SERVICE :: " + idService);
			logger.logTime("* [PROCESSING] Time", initTime, System.currentTimeMillis());
			logger.info("****************************************************************");
			
			
			new Thread() {
				public void run () {
					createProcessLog(pid, sb.toString(), logCodePath);
				}
			}.start();
			
			if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else
				return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing default command socket.", th);
			exitCode = 9988;
			exitCodeDescription = "Error executing default command socket";
			return action.findForward("error");
		}
		finally {
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			status.setMessageDescription(exitCodeDescription);
		}
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}
	
	public DefaultCommandSocket() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
	
	@SuppressWarnings("unchecked")
	private void createProcessLog(String pid, String text, String logPathService) {
		PrintWriter pw =null;
		try {
			// TODO Optimize and set in static File the logdir
			File logDir = null; 
			List<?> paths = GridConfiguration.getDefault().getList(ConfigurationParam.TC_LOGS_PATH);
			if(paths != null && paths.size() > 0){
				for (String path: (List<String>)paths) {
					if ((pid.startsWith("ppg") && path.contains("ppg")) ||  (pid.startsWith("bpg") && path.contains("bpg")))
						logDir = new File(path);
				}
			}
			logger.info("Process log directory :: " + logDir.getAbsolutePath()); 
				 
			String logName = pid.replace(":", "_") + ".log";
			
			pw = new PrintWriter(new File(logDir.getAbsoluteFile() + File.separator + logName));			
			pw.println(text);
			
			pw.println("Process log: "+logName+"\n");
			
			BufferedReader br = new BufferedReader(new FileReader(logPathService));
			String line = "";
			while ( (line = br.readLine() ) != null){
				pw.println(line);
			}
			pw.println("");
			br.close();
		}
		catch (Exception e) {
			logger.error("Error writing file " , e);
		}
		finally {
			try {				
				pw.flush();
				pw.close();				
			}
			catch(Throwable th) {
				logger.error("Error closing print writer" , th);
			}
			
		}
		
	}
}
