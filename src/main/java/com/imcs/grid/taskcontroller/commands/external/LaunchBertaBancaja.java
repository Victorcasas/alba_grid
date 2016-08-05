package com.imcs.grid.taskcontroller.commands.external;

import java.util.Properties;

import org.apache.axis2.AxisFault;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class LaunchBertaBancaja extends TaskControllerCommand {

	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.debug("INIT - LaunchBertaBancaja execution.");
		
		long timeStart = System.currentTimeMillis();
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String) action.getRequest().get("jcl");
		String step = (String) action.getRequest().get("step");
		String mode = (String) action.getRequest().get("mode");
		
		/* Specific Berta Bancaja parameters */
		String environment = (String)action.getRequest().get("environment");
		String modelName = (String)action.getRequest().get("name");
		
		logger.info("*********************************************************");
		logger.info("******* [PROCESSING] Init Launch Berta Bancaja **********");
		logger.info("*********************************************************");

		try {
			action.getResponse().put("mode", mode);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			String paramToPC = environment + " " +modelName;
			
			/* Call PC */
			ProcessConnectorBinding connector = new ProcessConnectorBinding("", "rexx LYNCE", "[" + paramToPC + "]", pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			logger.info("******************************************************");
			logger.info("[BERTABANCAJA] End launch Berta Bancaja with pid :: " + pid);
			logger.info("[BERTABANCAJA] Return Code :: " + exitCode);
			logger.info("[BERTABANCAJA] Exit Status Code :: \"" + exitStatusCode + "\"");
			logger.logTime("[BERTABANCAJA] Time", timeStart, System.currentTimeMillis());
			logger.info("******************************************************");

			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				logger.info("exitStatusCode KO!");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Berta Bancaja","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
				return action.findForward("error");
			} else if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or KO -> '" + exitStatusCode + "'");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing Berta Bancaja","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc","Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing Berta Bancaja","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc","Error connecting with Process Connector");
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error processing LaunchBertaBancaja.", th);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing Berta Bancaja","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9995");
			action.getResponse().put("gridrcDesc","Error executing Berta Bancaja service");
			return action.findForward("error");
		}
	}

	public void deploy() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
	
	public LaunchBertaBancaja() {
		logger.debug("Constructed " + getClass().getName());
	}

	public void checkInput(Action action) throws TaskControllerException {}

}