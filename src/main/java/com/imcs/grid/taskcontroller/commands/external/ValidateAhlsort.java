package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Properties;

import com.imcs.grid.commons.management.params.TaskControllerParam;
import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class ValidateAhlsort extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {		
		
		long timeStartSort = System.currentTimeMillis();	
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String) action.getRequest().get("pid");
		String jcl = (String) action.getRequest().get("jcl");
		String step = (String) action.getRequest().get("step");
		String sysin = (String) action.getRequest().get("sysin");
		
		/* File Names */
		FileInfo[] filesIn = (FileInfo[]) action.getRequest().get("files");
		FileInfo[] filesOut = (FileInfo[]) action.getRequest().get("filesOutput");
	
		try { 	
			logger.info("******************************************************");
			logger.info("[VALIDATEAHLSORT] Init validate sort with pid + " + pid); 
			logger.info("[VALIDATEAHLSORT] Parameters :: " + parameters);
			logger.info("******************************************************");
			
			/**
			 *  INIT - SORT (Ahlsort) Validation
			 */
			
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			// Get SYSIN without quotes 
			sysin = sysin.substring(1, sysin.length() - 1);
			
			// Sort work space defined in taskcontroller.xml 
			String workspaceSrt = TaskControllerParam.getInstance().getServiceWorkspaceDir("sort") + File.separator;
			
			// Generate source file with SYSIN from HOST 
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcFilename = workspaceSrt + "sort" + jcl + "-" + step + "-" + timestamp + ".src" ;
			generateSrcFile(filesIn, filesOut, sysin, srcFilename);
			
			// Directory where you run the command 
			String sortWorkingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(sortWorkingDir)) logger.warn("fc-working-dir is null or empty.");
			
			// Call Process connector: 
			//   workingDir : sortWorkingDir
			//   command : rexx Ahlsort_Pipe
			//   params:
			//      - sortCommand. java -jar <<path>>/ahlsort-X.X.jar
			//   	- operation: validate
			//      - inputFile. File with SYSIN from HOST
			//    pid 
			//    
			//*    Parameters example: -operation:validate -inputFile:E:/GRID/1.txt 
			//			
			String pcParameters = "[" + parameters.getProperty("sortCommand") + " -operation:validate"   
						+ " -inputfile:" + srcFilename + "] @outfilNames@"; 
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(sortWorkingDir, 
					"rexx Ahlsort_Pipe", pcParameters, pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			/**
			 *  END - SORT (Ahlsort) Validation
			 */
			
			logger.info("******************************************************");
			logger.info("[VALIDATEAHLSORT] End validate sort with pid :: " + pid); 
			logger.info("[VALIDATEAHLSORT] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[VALIDATEAHLSORT] Return Code :: " + exitCode);
			logger.info("[VALIDATEAHLSORT] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[VALIDATEAHLSORT] Time", timeStartSort, System.currentTimeMillis());
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
		catch (Throwable th) {
			logger.error("Error processing ValidateAhlsort.", th);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error executing Validate Ahlsort service");
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
		
		for (int i=0, iUntil = filesIn.length; i < iUntil; i++) { 
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
	
	@SuppressWarnings("unused")
	@Deprecated
	/*
	 * This was the method to generate SRC file when there was a treatment on the original SYSIN
	 */
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