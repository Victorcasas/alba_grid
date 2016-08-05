package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.util.Properties;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.types.Node;

public class CreateReport extends TaskControllerCommand {

	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.debug("INIT - CreateReport execution.");
		
		long timeStart = System.currentTimeMillis();
		
		/* Generic executions parameters */
		String workingDir = (String)parameters.getProperty("workingDir");
		String pid = (String)action.getRequest().get("pid");
		
		/* Specific report parameters */
		String initDateTime = (String)action.getRequest().get("initDate");
		String[] arrInitDateTime = initDateTime.split(" ");
		String initDate = "-initd:" + arrInitDateTime[0];
		String initTime = "-initt:" + arrInitDateTime[1];
		
		String endDateTime = (String)action.getRequest().get("endDate");
		String[] arrEndDateTime = endDateTime.split(" ");
		String endDate = "-endd:" + arrEndDateTime[0];
		String endTime = "-endt:" + arrEndDateTime[1];
		
		String operation = "-op:" + (String)action.getRequest().get("op");
		String outputFile = (String)action.getRequest().get("output");
		String command = (String)parameters.getProperty("command");
		String params = (String)parameters.getProperty("params");	
		String jclDemanded = (String)action.getRequest().get("jclDemanded");
		String jcl = null;
		if ((jclDemanded == null) || (jclDemanded.equals("")))
			jcl = "-jclDemanded:null";
		else
			jcl = "-jclDemanded:"+jclDemanded;
		String rootLocation = Node.getMyself().getRootLocation();
		String rootIp = "-rootip:" + rootLocation.substring(0, rootLocation.lastIndexOf(':'));
		String rootPort = "-rootport:" + rootLocation.substring(rootLocation.lastIndexOf(':') + 1);
		
		int rc = -1;

		/* BBDD parameters */
		GridConfiguration conf = GridConfiguration.getDefault();
		String url = "-urlDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_URL);
		String userName = "-userDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_USERNAME);
		String password = "-passDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_PASSWORD);
		String driver = "-driverDB:" + conf.getParameter(ConfigurationParam.MDS_BBDD_DRIVER);

		logger.info("*********************************************************");
		logger.info("* [PROCESSING] Init Create Report - Operation ::  " + operation);
		logger.info("*********************************************************");

		try {
			String extension = operation.contains("summaryReport") ? "xml" : "xlsx";
			String[] paramsResult = MdsProvider.getResultParams(pid, extension);
			String urlReference = paramsResult[0];
			if (jclDemanded.equals(""))
				urlReference = urlReference.replace("report",operation.substring(4));
			else 
				urlReference = urlReference.replace("report", "reportOnDemand_"+jclDemanded);
			
			String whereToSave = "-targetF:" + new File(".").getAbsolutePath() + File.separator + paramsResult[1].replace("report", operation.substring(4));
			if (!jclDemanded.equals(""))
				whereToSave = whereToSave.replace("customReport", "reportOnDemand_"+jclDemanded);
			String paramToPC = command + " " + params + " " + operation + " " + initDate + " " + initTime +
							   " " + endDate + " " + endTime + " " + " " + driver + " " + userName + " " + 
							   url + " " + whereToSave + " " + password + " " + jcl + " " + rootIp + " " + rootPort;
			
			/* If operation is summaryReport then takes one more parameter (outputFile) 
			 * If the outputFile field is null or empty the result is left in the default directory 
			 * (whereToSave). */
			paramToPC = operation.contains("summaryReport") ? paramToPC + " " + outputFile : paramToPC + " " + "null"; 
			
			/* Call PC */
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, "rexx GRID_Report", 
																			"[" + paramToPC + "]", pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			rc = output.getExitCode();
			
			if (rc == 0) {
				action.getResponse().put("description","Create report successfully.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				action.getResponse().put("mdsReference",urlReference);
				action.getResponse().put("recordToMds","true");
				return action.findForward("success");
			}
			else if (rc == 1) {
				action.getResponse().put("description","Create report with errors. Contact with your administrator.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				action.getResponse().put("mdsReference",urlReference);
				action.getResponse().put("recordToMds","true");
				return action.findForward("success");
			}
			else {
				logger.error("Error creating report. RC :: " + rc);
				action.getResponse().put("description","Could not create the file with the report. Contact with your administrator.");
				status.setMessageDescription("Create report Error.");
				action.getResponse().put("gridrc",String.valueOf(rc));
				//status.setStatus("terminated");
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
			logger.error("Error processing CreateReport.", th);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing createReport service");
			return action.findForward("error");
		}
		finally {
			logger.info("******************************************************");
			logger.info("************** End CreateReport **********************");
			logger.logTime("*** CreateReport Time ", timeStart, System.currentTimeMillis());
			logger.info("******************************************************");
		}
	}

	public void deploy() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
	
	public CreateReport() {
		logger.debug("Constructed " + getClass().getName());
	}

	public void checkInput(Action action) throws TaskControllerException {}

}