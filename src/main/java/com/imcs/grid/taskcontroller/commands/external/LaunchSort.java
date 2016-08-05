package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

import org.apache.axis2.AxisFault;

import com.imcs.grid.commons.management.params.TaskControllerParam;
import com.imcs.grid.commons.management.params.TaskControllerParamException;
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
import com.imcs.grid.taskcontroller.util.StatisticsUtils;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.FileInfo;

public class LaunchSort extends TaskControllerCommand {

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
		
		/* Specific sort parameters */
		String commandSort = (String)parameters.getProperty("sortCommand");
		
		/* File Names */
		FileInfo[] filesIn = (FileInfo[])action.getRequest().get("files");
		FileInfo[] filesOut = (FileInfo[])action.getRequest().get("filesOutput");
		
		try { 	
			logger.info("******************************************************");
			logger.info("[SORT] Init execute sort with pid " + pid); 
			logger.info("[SORT] Parameters :: " + parameters);
			logger.info("******************************************************");
			
			action.getResponse().put("mode", mode);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Specific SYNCSORT parameters from taskcontroller-flow.xml*/
			List<String> syncsortParams = getSyncsortParameters(parameters);
						
			/* Get SYSIN without quotes */
			sysin = sysin.substring(1,sysin.length()-1);
		
			/* Delete SYNCSORT work spaces (specific parameter workspace1 and workspace1). 
			   If an error occurs the service is not stopped */
			
			String[] workSpaces = {setPort(parameters.getProperty("workspace1")),
								   setPort(parameters.getProperty("workspace2"))};
			deleteWorkSpaces(workSpaces);	
			
			/* Sort work space defined in taskcontroller.xml */
			String workspaceSrt = TaskControllerParam.getInstance().getServiceWorkspaceDir("sort") + File.separator;
			
			/* Generate source file with input and output files and SYSIN */
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcFilename = workspaceSrt + "sort" + jcl + "-" + step + "-" + timestamp + ".src" ;
			generateSrcFile(filesIn,filesOut,sysin,srcFilename);
			
			/* Generate target file name to write SYNCSORT code */
			String targetFile = workspaceSrt + "sort" + jcl + "-" + step + "-" + timestamp + ".srt" ;
			
			/* Directory where you run the command */
			String sortWorkingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(sortWorkingDir)) logger.warn("fc-working-dir is null or empty.");
			
			/* Call Process connector: 
			 *   workingDir : sortWorkingDir
			 *   command : rexx Sort_Pipe
			 *   params:
			 *      - sortCommand. java -jar <<path>>/sort-X.X.jar
			 *   	- operation: translate 
			 *      - inputFile. File with input and output files and SYSIN from HOST
			 *      - SYNCSORT parameters
			 *   	- outputFile. File with translation to SYNCSORT code
			 *    pid 
			 *    
			 *    Parameters example: -operation:translate -inputFile:E:/GRID/1.txt -outputFile:E:/GRID/1.srt 
			 *                        -statistics:yes -workspace=c:/ 
			 * */
			String pcParameters = "[" + commandSort + " -operation:translate" + " -inputFile:" + srcFilename;
			for (int i=0, iUntil = syncsortParams.size();i<iUntil;i++) 
				pcParameters += " " + syncsortParams.get(i);
			
			pcParameters += " -outputFile:" + targetFile + "]";
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(sortWorkingDir, 
					"rexx Sort_Pipe", pcParameters, pid);
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
			
			/* Get Command Log from Process connector */
			String resCmd = ProcessConnectorMng.getInstance().getFullOutput(output.getOutputPath());
			status.setLogResult(resCmd);
			
			logger.info("******************************************************");
			logger.info("[SORT] End execute sort with pid :: " + pid); 
			logger.info("[SORT] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[SORT] Return Code :: " + exitCode);
			logger.info("[SORT] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[SORT] Time", timeStartSort, System.currentTimeMillis());
			logger.info("******************************************************");
			
			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
				return action.findForward("error");
			}
			else if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or " +
						"KO -> '" + exitStatusCode + "'");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc", "Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		} catch (TaskControllerParamException tcex) {
			logger.error("Error getting parameters used by the service.",tcex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9998");
			action.getResponse().put("gridrcDesc", "An error occurred due to the parameters used by the service.");
			return action.findForward("error");
		} catch (IOException ioex) {
			logger.error("Error creating source file with code of Syncsort",ioex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9997");
			action.getResponse().put("gridrcDesc", "An error occurred due to the source file used by the service.");
			return action.findForward("error");
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with Process Connector.", pcex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector ");
			return action.findForward("error");
		} catch (NumberFormatException nfex) {
			logger.error("Error getting alebra statistics.", nfex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9991");
			action.getResponse().put("gridrcDesc", "Error getting alebra statistics.");
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error processing LaunchSort.", th);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Sort","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch Sort");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing Sort service");
			return action.findForward("error");
		}
	}

	/**
	 * This method deletes the work spaces defined in taskcontroller-flow.xml. 
	 * If it can not delete or get an error then write a warning and continues execution.
	 * 
	 * @param workSpaces
	 */
	private void deleteWorkSpaces(String[] workSpaces) {
		for (int k=0,kHasta=workSpaces.length;k<kHasta;k++) {
			try {
				String path = workSpaces[k].substring(workSpaces[k].indexOf("\"")+1);
				logger.info("Deleting workspace " + path.substring(0,path.length()-1));
				File dirWorkSpace = new File (path.substring(0,path.length()-1));
				
				//If not exist, create directory
				if (!dirWorkSpace.exists() || !dirWorkSpace.isDirectory())
					dirWorkSpace.mkdir();
				
				File[] filesWorSpace = dirWorkSpace.listFiles();
				for (int l=0,lHasta=filesWorSpace.length;l<lHasta;l++) {
					if ((filesWorSpace[l].getName().toLowerCase().endsWith(".tmp")) && (filesWorSpace[l].getName().toLowerCase().startsWith("t$")))							
						filesWorSpace[l].delete();
				}
			} catch(Throwable t) {
				logger.warn("Error deleting sort workspace " + k, t);
			}
		}
	}
	
	/**
	 * Gets SYNCSORT parameters from taskcontroller-flow.xml
	 * 
	 * @param parameters
	 * @return List of String with parameters
	 */
	private List<String> getSyncsortParameters(Properties parameters) {
		List<String> params = new ArrayList<String>();
		params.add("-statistics:" + parameters.getProperty("statistics"));
		params.add("-silent:" + parameters.getProperty("silent"));
		params.add("-prompt:" + parameters.getProperty("prompt"));
		params.add("-warnings:" + parameters.getProperty("warnings"));
		params.add("-memory:" + parameters.getProperty("memory"));
		int i = 1;
		while (parameters.getProperty("workspace" + i) != null) {
			params.add("-workspace:" + setPort(parameters.getProperty("workspace" + i)));
			i++;
		}
		return params;
	}
	
	/**
	 * Generates a file with input files, output files and SYSIN
	 * 
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
		
		for (int i=0, iUntil = filesIn.length; i<iUntil; i++)
			pw.write("INFILE " + filesIn[i].getName() + " " + filesIn[i].getRecordLen() + eol);
		
		for (int i=0, iUntil = filesOut.length; i<iUntil; i++)
			pw.write("OUTFILE " + filesOut[i].getName() + " " + filesOut[i].getRecordLen() + eol);
		
		pw.write(eol + eol + sysin);
		pw.flush();
		pw.close();
	}
	
	private String setPort(String directory) {
		// Remove last quotes
		String path = directory.substring(0, directory.length()-1);
		// Insert port
		String port = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_BINDING_PORT);
		return path + File.separator + port + "\"";
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}