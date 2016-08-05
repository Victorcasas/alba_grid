package com.imcs.grid.taskcontroller.commands.commons;

import java.util.Properties;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.UtilString;

public class DefaultCommand extends TaskControllerCommand
{

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException
	{
		long initTime = System.currentTimeMillis();
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String idService = (String)action.getRequest().get("idService");
		logger.info("ID SERVICE :: " + idService);
		try {	
			logger.info("*********************************************************");
			logger.info("* [PROCESSING] Init DEFAULT COMMAND FROM SERVICE :: " + idService);
			logger.info("*********************************************************");
			
			/* Get XML message parameters */
			String xmlParam = (String) action.getRequest().get("parameters");
			/* Directory where you run the command */
			String workingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(workingDir)) logger.warn("fc-working-dir is null or empty.");
			logger.info("Execution working dir :: " + workingDir);
			
			/* Get the command to be executed */
			String command = parameters.getProperty("command");
			logger.info("*********************************************************");
			logger.info("* [PROCESSING] Executing command :: " +command);
			logger.info("* [PROCESSING] With Parameters   :: " +xmlParam);
			logger.info("*********************************************************");
			/* Call process connector */
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, command, xmlParam, pid);
			ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
			pcMng.addProcess(pid, connector);
			pcMng.startProcess(connector);
			ShellCommandOutput output = pcMng.monitor(connector);
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			logger.info("*********************************************************");
			logger.info("* [PROCESSING] End DEFAULT COMMAND FROM SERVICE :: " + idService);
			logger.logTime("* [PROCESSING] Time", initTime, System.currentTimeMillis());
			logger.info("*********************************************************");
			
			if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else
				return action.findForward("error");
		}
		catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			exitCode = 9996;
			exitCodeDescription = "Error connecting with Process Connector";
			return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing default command.", th);
			exitCode = 9988;
			exitCodeDescription = "Error executing default command";
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
	
	public DefaultCommand() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
}
