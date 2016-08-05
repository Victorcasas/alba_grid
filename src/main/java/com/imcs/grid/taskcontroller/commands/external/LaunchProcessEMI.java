package com.imcs.grid.taskcontroller.commands.external;

import java.util.Properties;
import org.apache.axis2.AxisFault;
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
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.UtilString;

public class LaunchProcessEMI extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {

		long timeStartProcess = System.currentTimeMillis();

		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;

		/* Generic executions parameters */
		String pid = (String) action.getRequest().get("pid");
		String jcl = (String) action.getRequest().get("jcl");
		String step = (String) action.getRequest().get("step");
//		String idExecution = (String) action.getRequest().get("idExecution");

		/* File Names */

		try {
			logger.info("******************************************************");
			logger.info("[PROCESS_EMI] Init execute process EMI with pid " + pid);
			logger.info("[PROCESS_EMI] Parameters :: " + parameters);
			logger.info("******************************************************");

			action.getResponse().put("exitStatusCode", exitStatusCode);

			/* Directory where you run the command */
			String workingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(workingDir)) logger.warn("fc-working-dir is null or empty.");
			String dburl =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_URL);
			String dbuser =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_USERNAME);
			String dbpasswd =  GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_BBDD_PASSWORD);

			String database = dburl.split("//")[1];
			String dbip = database.split(":")[0];
			String dbport = database.split(":")[1].split("/")[0];
			String dbname = database.split(":")[1].split("/")[1];
			String[] params = jcl.split("_"); 
			String command = parameters.getProperty("command");
			String emiLogDir = parameters.getProperty("emiLogDir");
			String pcParameters = "";
			for (String s : params) {
				pcParameters += s + " ";
			}

			pcParameters += emiLogDir + " ";
			pcParameters += dbname + " ";
			pcParameters += dbip + " ";
			pcParameters += dbport + " ";
			pcParameters += dbuser + " ";
			pcParameters += dbpasswd + " ";
			
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, command, pcParameters, pid);
			ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
			pcMng.addProcess(pid, connector);
			pcMng.startProcess(connector);
			ShellCommandOutput output = pcMng.monitor(connector);
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();

			/* Get Alebra statistics */
//			try {
//				StatisticsUtils.setAlebraStatistics(connector, jcl, step, idExecution, pid);
//			} catch (AxisFault e) {
//				logger.error("Error adding Alebra statistics :: " + e);
//			} catch (InterruptedException e) {
//				logger.error("Error adding Alebra statistics:: " + e);
//			}

			logger.info("******************************************************");
			logger.info("[PROCESS_EMI] End execute process EMI with pid :: " + pid);
			logger.info("[PROCESS_EMI] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[PROCESS_EMI] Return Code :: " + exitCode);
			logger.info("[PROCESS_EMI] Exit Status Code :: \"" + exitStatusCode + "\"");
			logger.logTime("[PROCESS_EMI] Time", timeStartProcess, System.currentTimeMillis());
			logger.info("******************************************************");

			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			status.setMessageDescription(exitCodeDescription);

			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				logger.info("exitStatusCode KO!");
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch process EMI","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
				return action.findForward("error");
			} else if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or "
								+ "KO -> '" + exitStatusCode + "'");

				try {
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing process EMI","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
				
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc","Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			try{
				TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing process EMI","","","");
			} catch (AxisFault e) {
				logger.error("Error adding result parallel :: " + e);
			} catch (InterruptedException e) {
				logger.error("Error adding result parallel :: " + e);
			}
		
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc","Error connecting with Process Connector");
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error processing LaunchCobolEMI.", th);
			try{
				TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing process EMI","","","");
			} catch (AxisFault e) {
				logger.error("Error adding result parallel :: " + e);
			} catch (InterruptedException e) {
				logger.error("Error adding result parallel :: " + e);
			}
			
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9995");
			action.getResponse().put("gridrcDesc","Error executing Cobol EMI service");
			return action.findForward("error");
		}
	}

	
	public void checkInput(Action actions) throws TaskControllerException {
	}

	public void loadConfiguration() throws TaskControllerException {
	}

	public void install() throws TaskControllerException {
	}

	public void deploy() throws TaskControllerException {
	}

	public void undeploy() throws TaskControllerException {
	}
}