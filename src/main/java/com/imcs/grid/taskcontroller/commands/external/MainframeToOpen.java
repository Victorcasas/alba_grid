package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Properties;

import org.apache.axis2.AxisFault;

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
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.FileInfo;

public class MainframeToOpen extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {

		long timeStartService = System.currentTimeMillis();
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String mode = (String)action.getRequest().get("mode");
		
		/* Specific MAINFRAMETOOPEN parameters */
		String rexxCommand = (String)parameters.getProperty("rexxCommand");
		String targetDir = (String)parameters.getProperty("targetDir");
		
		/* File Names */
		FileInfo[] files = (FileInfo[])action.getRequest().get("files");

		try { 	
			logger.info("**********************************************************");
			logger.info("[MAINFRAMETOOPEN] Init execute mainframeToOpen with pid + " + pid); 
			logger.info("[MAINFRAMETOOPEN] Parameters :: " + parameters);
			logger.info("**********************************************************");
			
			if (files.length > 0) {
				
				/* Download files work space defined in taskcontroller.xml */
				String workspaceDownload = TaskControllerParam.getInstance().getServiceWorkspaceDir("download") + File.separator;
				
				/* Generate source file with input files */
				long timestamp = Calendar.getInstance().getTimeInMillis();
				String srcFilename = workspaceDownload + "download" + jcl + "-" + step + "-" + timestamp + ".mto" ;
				generateSrcFile(files, targetDir, srcFilename);
			
				/* Directory where you run the command */
				String ioWorkingDir = (String) action.getRequest().get("fc-working-dir");
				if (UtilString.isNullOrEmpty(ioWorkingDir)) logger.warn("fc-working-dir is null or empty.");
				
				/*
				 * Call Process connector: 
				 * 	 workingDir : ioWorkingDir 
				 * 	 command : rexx Get_Alebra ascii
				 *   params: 
				 *      - srcFilename: File name with input files to download
				 *   pid
				 */
				String pcParameters = "[" + srcFilename + "]";
				ProcessConnectorBinding connector = new ProcessConnectorBinding(ioWorkingDir, 
														rexxCommand, pcParameters, pid);
				ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
				pcMng.addProcess(pid, connector);
				pcMng.startProcess(connector);
				ShellCommandOutput output = pcMng.monitor(connector);
				
				exitCode = output.getExitCode();
				exitStatusCode = output.getExitStatusCode();
				exitCodeDescription = output.getExitCodeDescription();
			}
			else {
				logger.info("[MAINFRAMETOOPEN] There are no files to download");
				exitStatusCode = "OK";
			}
			
			logger.info("******************************************************");
			logger.info("[MAINFRAMETOOPEN] End execute mainframetoopen with pid :: " + pid); 
			logger.info("[MAINFRAMETOOPEN] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[MAINFRAMETOOPEN] Return Code :: " + exitCode);
			logger.info("[MAINFRAMETOOPEN] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[MAINFRAMETOOPEN] Time", timeStartService, System.currentTimeMillis());
			logger.info("******************************************************");
			
			action.getResponse().put("files", action.getRequest().get("files"));
			if (action.getRequest().containsKey("filesMod"))
				action.getResponse().put("filesMod", action.getRequest().get("filesMod"));
			if (action.getRequest().containsKey("filesOriginal"))
				action.getResponse().put("filesOriginal", action.getRequest().get("filesOriginal"));
			
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
				return action.findForward("error");
			}
			else if (exitStatusCode.equalsIgnoreCase("OK")) {
				status.setMessageDescription("Files downloaded.");
				return action.findForward("success");
			}
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or " +
						"KO -> '" + exitStatusCode + "'");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc", "Error getting the exit status code for the service.");
				return action.findForward("error");
			}
			
		}
		catch (TaskControllerParamException tcex) {
			logger.error("Error getting parameters used by the service.",tcex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9998");
			action.getResponse().put("gridrcDesc", "An error occurred due to the parameters used by the service.");
			return action.findForward("error");
		}
		catch (IOException ioex) {
			logger.error("Error creating source file with input files",ioex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9997");
			action.getResponse().put("gridrcDesc", "An error occurred due to the source file used by the service.");
			return action.findForward("error");
		}
		catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with Process Connector.", pcex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector ");
			return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing Mainframetoopen.", th);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Download files","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Download files");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing download files");
			return action.findForward("error");
		}
	}

	/**
	 * Generates a file with input files
	 * 
	 * @param filesIn      : List of FileInfo with all input files.
	 * @param srcFilename  : Source file name 
	 * @return
	 * @throws IOException
	 */
	private void generateSrcFile(FileInfo[] files, String targetDir,String srcFilename) throws IOException {	
		FileWriter file = new FileWriter(srcFilename);
		PrintWriter pw = new PrintWriter(file);
		String eol = System.getProperty("line.separator");
		for (int i=0, iUntil = files.length; i<iUntil; i++)
			pw.write(files[i].getName() +  " " +  targetDir + files[i].getName() + eol);
		pw.close();
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}