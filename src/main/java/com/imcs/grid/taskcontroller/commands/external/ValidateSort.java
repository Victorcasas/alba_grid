package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Properties;

import com.imcs.grid.commons.management.params.TaskControllerParam;
import com.imcs.grid.commons.management.params.TaskControllerParamException;
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

public class ValidateSort extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {		
		
		long timeStartSort = System.currentTimeMillis();	
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String sysin = (String)action.getRequest().get("sysin");

		
		try { 	
			logger.info("******************************************************");
			logger.info("[VALIDATESORT] Init validate sort with pid + " + pid); 
			logger.info("[VALIDATESORT] Parameters :: " + parameters);
			logger.info("******************************************************");
			
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Get SYSIN without quotes */
			sysin = sysin.substring(1,sysin.length()-1);
			
			/* Sort work space defined in taskcontroller.xml */
			String workspaceSrt = TaskControllerParam.getInstance().getServiceWorkspaceDir("sort") + File.separator;
			
			/* Generate source file with SYSIN from HOST */
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcFilename = workspaceSrt + "sort" + jcl + "-" + step + "-" + timestamp + ".src" ;
			generateSrcFile(sysin,srcFilename);
			
			/* Directory where you run the command */
			String sortWorkingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(sortWorkingDir)) logger.warn("fc-working-dir is null or empty.");
			
			/* Call Process connector: 
			 *   workingDir : sortWorkingDir
			 *   command : rexx Sort_Pipe
			 *   params:
			 *      - sortCommand. java -jar <<path>>/sort-X.X.jar
			 *   	- operation: validate
			 *      - inputFile. File with SYSIN from HOST
			 *    pid 
			 *    
			 *    Parameters example: -operation:validate -inputFile:E:/GRID/1.txt 
			 * */			
			String pcParameters = "[" + parameters.getProperty("sortCommand") + " -operation:validate"   
						+ " -inputFile:" + srcFilename + "]"; 
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(sortWorkingDir, 
					"rexx Sort_Pipe", pcParameters, pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			/*
			exitCode = 0;
			exitStatusCode = "OK";
			exitCodeDescription = "Validation forced to be OK";
			*/
			
			logger.info("******************************************************");
			logger.info("[VALIDATESORT] End validate sort with pid :: " + pid); 
			logger.info("[VALIDATESORT] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[VALIDATESORT] Return Code :: " + exitCode);
			logger.info("[VALIDATESORT] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[VALIDATESORT] Time", timeStartSort, System.currentTimeMillis());
			logger.info("******************************************************");
			
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				status.setMessageDescription("Error validate");
				return action.findForward("error");
			}
			else if (exitStatusCode.equalsIgnoreCase("OK")) {
				status.setMessageDescription("ok");
				return action.findForward("success");
			}
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or " +
						"KO -> '" + exitStatusCode + "'");
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc", "Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		}
		catch (TaskControllerParamException tcex) {
			logger.error("Error getting parameters used by the service.",tcex);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9998");
			action.getResponse().put("gridrcDesc", "An error occurred due to the parameters used by the service.");
			return action.findForward("error");
		}
		catch (IOException ioex) {
			logger.error("Error creating source file with SYSIN",ioex);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9997");
			action.getResponse().put("gridrcDesc", "An error occurred due to the source file used by the service.");
			return action.findForward("error");
		}
		catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with Process Connector.", pcex);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector ");
			return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing ValidateSort.", th);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing Validate Sort service");
			return action.findForward("error");
		}
	}
	
	private void generateSrcFile(String sysin, String filename) throws IOException {
		FileWriter file = new FileWriter(filename);
		PrintWriter pw = new PrintWriter(file);
		pw.write(sysin);
		pw.close();
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}
	
	public void loadConfiguration() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
}