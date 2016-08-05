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
import com.imcs.grid.taskcontroller.util.StatisticsUtils;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class LaunchAhlsort extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		
		long timeStartSort = System.currentTimeMillis();
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String mode = (String)action.getRequest().get("mode");
		String sysin = (String)action.getRequest().get("sysin");
		String idExecution = (String)action.getRequest().get("idExecution");
		
		/* Specific ahlsort parameters */
		String ahlCommand = (String)parameters.getProperty("ahlCommand");
		String core = (String)parameters.getProperty("core");
		
		/* File Names */
		FileInfo[] filesIn = (FileInfo[])action.getRequest().get("files");
		FileInfo[] filesOut = (FileInfo[])action.getRequest().get("filesOutput");
		
		try {
			logger.info("******************************************************");
			logger.info("[AHLSORT] Init execute ahlsort with pid " + pid); 
			logger.info("[AHLSORT] Parameters :: " + parameters);
			logger.info("******************************************************");
			
			action.getResponse().put("mode", mode);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Get SYSIN without quotes */
			sysin = sysin.substring(1,sysin.length()-1);
						
			/* Sort work space defined in taskcontroller.xml */
			String workspaceSrt = TaskControllerParam.getInstance().getServiceWorkspaceDir("sort") + File.separator;
			
			/* Generate source file with input and output files and SYSIN */
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcFilename = workspaceSrt + "ahlsort" + jcl + "-" + step + "-" + timestamp + ".src";
			generateSrcFile(filesIn,filesOut,sysin,srcFilename);
			
			/* Generate target file name to write AHLSORT code */
			String targetFile = workspaceSrt + "ahlsort" + jcl + "-" +step + "-" + timestamp + ".ctl";
			
			/* Directory where you run the command */
			String sortWorkingDir = (String)action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(sortWorkingDir))
				logger.warn("fc-working-dir is null or empty.");
			
			/* Call Process connector */
			String pcParameters = "["+ahlCommand+" -operation:translate -core:"+core+" -inputfile:"+srcFilename+" -outputfile:"+targetFile+"] " +
					"@outfilNames@ "+getOutfilNamesAndLength(sysin,filesOut);
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(sortWorkingDir, "rexx Ahlsort_Pipe", pcParameters, pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			/* Set ALEBRA statistics */
			try {
				StatisticsUtils.setAlebraStatistics(connector, jcl, step, idExecution, pid);
			} catch (AxisFault e) {
				logger.error("Error adding Alebra statistics :: " + e);
			} catch (InterruptedException e) {
				logger.error("Error adding Alebra statistics :: " + e);
			}
			
			/* Get Command Log from Process Connector */
			String resCmd = ProcessConnectorMng.getInstance().getFullOutput(output.getOutputPath());
			status.setLogResult(resCmd);
			
			logger.info("******************************************************");
			logger.info("[AHLSORT] End execute sort with pid :: " + pid); 
			logger.info("[AHLSORT] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[AHLSORT] Return Code :: " + exitCode);
			logger.info("[AHLSORT] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[AHLSORT] Time", timeStartSort, System.currentTimeMillis());
			logger.info("******************************************************");
			
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try {
						TaskControllerToBrokerClient.callAddResultParallel(jcl, step, "", "", "Error - Launch Ahlsort","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
				return action.findForward("error");
			} else if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or " +
						"KO -> '" + exitStatusCode + "'");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try {
						TaskControllerToBrokerClient.callAddResultParallel(jcl, step, "", "", "Error - Launch Ahlsort","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}				
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc", "Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		} catch (TaskControllerParamException tcex) {
			logger.error("Error getting parameters used by the service.",tcex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Ahlsort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9998");
			action.getResponse().put("gridrcDesc", "An error occurred due to the parameters used by the service.");
			return action.findForward("error");
		} catch (IOException ioex) {
			logger.error("Error creating source file with code of Ahlsort",ioex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Ahlsort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9997");
			action.getResponse().put("gridrcDesc", "An error occurred due to the source file used by the service.");
			return action.findForward("error");
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with Process Connector.", pcex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Ahlsort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector ");
			return action.findForward("error");
		} catch (NumberFormatException nfex) {
			logger.error("Error getting alebra statistics.", nfex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Ahlsort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9991");
			action.getResponse().put("gridrcDesc", "Error getting alebra statistics.");
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error processing LaunchSort.", th);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Ahlsort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing Sort service");
			return action.findForward("error");
		}
	} 
	
	/**
	 * Generates a file with input files, output files and SYSIN
	 * @param filesIn      : List of FileInfo with all input files.
	 * @param filesOut	   : List of FileInfo with all output files.
	 * @param sysin
	 * @param srcFilename  : Source file name 
	 * @return
	 * @throws IOException
	 */
	private void generateSrcFile(FileInfo[] filesIn, FileInfo[] filesOut, String sysin, String srcFilename) throws IOException {	
		FileWriter file = new FileWriter(srcFilename);
		PrintWriter pw = new PrintWriter(file);
		String eol = System.getProperty("line.separator");
		String type = "";
		
		for (int i=0, iUntil = filesIn.length; i<iUntil; i++) { 
			if (filesIn[i].getDcb().contains("RECFM=VB"))
				type = "VARIABLE";
			else
				type = "FIXED";
			pw.write("INFILE " + filesIn[i].getLabel() + " " + filesIn[i].getName() + " " + type + " " + filesIn[i].getRecordLen() + eol);
		
		}
		for (int i=0, iUntil = filesOut.length; i<iUntil; i++) {
			if (filesOut[i].getDcb().contains("RECFM=VB")) 
				type = "VARIABLE";
			else
				type = "FIXED";
			pw.write("OUTFILE " + filesOut[i].getLabel() + " " + filesOut[i].getName() + " " + type + " " + filesOut[i].getRecordLen() + eol);
		}
		pw.write(eol + eol + sysin);
		pw.flush();
		pw.close();
	}
	
	private String getOutfilNamesAndLength(String sysin, FileInfo[] filesOut) {
		String str = sysin, outfilNames = "";
		boolean end = false;
		int index, fileNum=0;
		
		while (!end) {
			if (str.contains("OUTFIL FNAMES=")) {
				str = str.substring(str.indexOf("OUTFIL FNAMES=")+14);
				index = 0;
				if (str.charAt(index) == '(')
					index++;
				while ((str.charAt(index) != ' ') && (str.charAt(index) != ',') && (str.charAt(index) != '\n') && (str.charAt(index) != '\t') 
						&& (str.charAt(index) != ')')) {
					outfilNames += str.charAt(index);
					index++;
				}
				outfilNames += ","+filesOut[fileNum].getRecordLen().trim()+",";
				fileNum++;
			} else {
				end = true;
			}
		}
		if (!outfilNames.equals(""))
			outfilNames = outfilNames.substring(0,outfilNames.length()-1);
		return outfilNames;
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}
	
	public void loadConfiguration() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
}
