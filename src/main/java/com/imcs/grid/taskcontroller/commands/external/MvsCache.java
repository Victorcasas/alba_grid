package com.imcs.grid.taskcontroller.commands.external;

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
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;

public class MvsCache extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		long initTime = System.currentTimeMillis();
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "default";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String idService = (String)action.getRequest().get("idService");
		
		/* Specific MvsCache parameters */
		String cachePath = (String)parameters.getProperty("cachePath");
		String cacheMaxDir = (String) parameters.getProperty("cacheMaxDir");
		String cacheMinTime = (String) parameters.getProperty("cacheMinTime");
		
		try {
			logger.info("******************************************************");
			logger.info("[MvsCache] Init execute " + idService + " with pid " + pid); 
			logger.info("[MvsCache] Parameters :: " + parameters);
			logger.info("******************************************************");
		
			String type = (String)action.getRequest().get("type");
			String jclName = (String)action.getRequest().get("jclName");
			String workingDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.MAIN_PATH) + TaskcontrollerNS.USERSERVICES.getValue();
			
			String dburl =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_URL);
			String dbuser =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_USERNAME);
			String dbpasswd =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_PASSWORD);

			String scriptParams = "";
			String scriptCommand = "";
			
			if (type.equals("analysis")) {	
				String jcl = (String)action.getRequest().get("jclMsg");
				scriptCommand =  "/usr/bin/python3 JCL_Parser.py";
				scriptParams = "--analysis "+ cachePath + " " + jclName + " " + cacheMaxDir + " " 
								+ dburl + " " + dbuser + " " + dbpasswd + " " + cacheMinTime + " " +jcl;
			
			}else if (type.equals("init")) {	
				String jcl = (String)action.getRequest().get("jclMsg");
				scriptCommand =  "/usr/bin/python3 JCL_Parser.py";
				scriptParams = cachePath + " " + jclName + " " + cacheMaxDir + " " 
								+ dburl + " " + dbuser + " " + dbpasswd + " " + cacheMinTime + " " +jcl;
			
			} else if (type.equals("end")) {
				scriptCommand = "/usr/bin/python3 delete_Cache.py";
				scriptParams = cachePath + " " + jclName + " "
								+ dburl + " " + dbuser + " " + dbpasswd;
			}
			
			logger.info("*** Execute command :: "+ scriptCommand + " " + scriptParams);
			/* Call process connector */
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, scriptCommand, scriptParams, pid);
			ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
			pcMng.addProcess(pid, connector);
			pcMng.startProcess(connector);
			ShellCommandOutput output = pcMng.monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			logger.info("*********************************************************");
			logger.info("* [MvsCache] End execute " + idService + " with pid " + pid);
			logger.logTime("* [MvsCache] Time", initTime, System.currentTimeMillis());
			logger.info("*********************************************************");
			
			if (exitStatusCode.equalsIgnoreCase("OK")) {
				return action.findForward("success");
			} else {
				return action.findForward("error");
			}
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			exitCode = 9996;
			exitCodeDescription = "Error connecting with Process Connector";
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error executing mvs cache service.", th);
			exitCode = 9998;
			exitCodeDescription = "Error executing mvs cache service.";
			return action.findForward("error");
		} finally {
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			status.setMessageDescription(exitCodeDescription);
		}
	}

	public void checkInput(Action action) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
	
	
}
