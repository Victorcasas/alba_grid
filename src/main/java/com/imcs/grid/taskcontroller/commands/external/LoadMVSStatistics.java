package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.util.Properties;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class LoadMVSStatistics extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.debug("INIT - LoadMVSStatistics execution.");
		
		long timeStart = System.currentTimeMillis();
		
		/* Generic executions parameters */
		String workingDir = (String)parameters.getProperty("workingDir");
		String pid = (String)action.getRequest().get("pid");
		String rexxCommand = (String) action.getRequest().get("loadMvsStatisticsRexx"); 
		
		/* Specific service parameters */
		String operation = "-op:loadDatabase";
		String command = (String)parameters.getProperty("command");
	//	String params = (String)parameters.getProperty("params");
		String tmpDir = (String)parameters.getProperty("tmpDir");
		String delete = (String)parameters.getProperty("deleteTmpFiles");
		
		int rc = -1;

		/* BBDD parameters */
		GridConfiguration conf = GridConfiguration.getDefault();
		String url = "-urlDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_URL);
		String userName = "-userDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_USERNAME);
		String password = "-passDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_PASSWORD);
		String driver = "-driverDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_DRIVER);
		
		logger.info("*********************************************************");
		logger.info("* [PROCESSING] Init Load MVS Statistics - Operation ::  " + operation);
		logger.info("*********************************************************");
		
		try {
			String whereToSave = "-targetF:" + tmpDir + File.separator + "mvsstatistics-" + System.currentTimeMillis() + ".txt";
			
			String paramToPC = command + " " + operation + " " + driver + " " + userName + " " + 
							   url + " " + password + " " + whereToSave; 
			/* Call PC */
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, rexxCommand, 
																			"[" + paramToPC + "]", pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			rc = output.getExitCode();
			
			if (rc == 0) {
				action.getResponse().put("description","MVS statistics loaded successfully");
				action.getResponse().put("gridrc",String.valueOf(rc));
				if (delete.equalsIgnoreCase("yes"))
					deleteFile(whereToSave);
				return action.findForward("success");
			}
			else if (rc == 1) {
				action.getResponse().put("description","Could not load all the MVS statistics. Contact with your system administrator.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				return action.findForward("success");
			}
			else if (rc == 2) {
				action.getResponse().put("description","MVS statistics exist in database.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				if (delete.equalsIgnoreCase("yes"))
					deleteFile(whereToSave);
				return action.findForward("success");
			}
			else if (rc == 3) {
				action.getResponse().put("description","There are no statistics in MVS file.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				return action.findForward("success");
			}
			else {
				logger.error("Error loading MVS statistics. RC :: " + rc);
				action.getResponse().put("description","Error loading MVS statistics. Contact with your administrator.");
				status.setMessageDescription("Error loading MVS statistics.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				return action.findForward("error");
			}
		}
		catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector");
			return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing LoadMVSStatistics.", th);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing LoadMVSStatistics service");
			return action.findForward("error");
		}
		finally {
			logger.info("***********************************************************");
			logger.info("************** End LoadMVSStatistics **********************");
			logger.logTime("*** LoadMVSStatistics Time ", timeStart, System.currentTimeMillis());
			logger.info("***********************************************************");
		}
	}
	
	private void deleteFile(String filename)
	{
		try {
			new File(filename).delete();
		}
		catch (Throwable th) {
			logger.warn("File " + filename + " is not deleted");
		}
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}

}
