package com.imcs.grid.taskcontroller.commands.external;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class CompileCobol extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		
		long timeStartSort = System.currentTimeMillis();	
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String rexxCommand = (String) action.getRequest().get("compilecobolRexx"); 
		String timestampPGM = (String) action.getRequest().get("timestamp");
		
		/* Specific compile COBOL parameters */
		String executionPath = (String)parameters.getProperty("executionPath");
		String sourcePath = (String)parameters.getProperty("sourcePath");
		
		/* File Names */
		FileInfo[] sourceFiles = (FileInfo[])action.getRequest().get("files");
		
		try { 	
			logger.info("******************************************************");
			logger.info("[COMPILECOBOL] Init execute compile cobol with pid " + pid); 
			logger.info("[COMPILECOBOL] Parameters :: " + parameters);
			logger.info("******************************************************");
			
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Cobol work space defined in taskcontroller.xml */
			String workspaceCbl = TaskControllerParam.getInstance().getServiceWorkspaceDir("cobol") + File.separator;

			/* Generate source file with source files */
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcFilename = workspaceCbl + "compilecobol" + jcl + "-" + step + "-" + timestamp + ".src" ;
			generateCompileCobolFile(sourceFiles,srcFilename);
			
			/* Generate target file name to write result */
			String targetFile = workspaceCbl + "compilecobol" + jcl + "-" + step + "-" + timestamp + ".comp" ;
			
			/* Directory where you run the command */
			String compileCobolWorkingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(compileCobolWorkingDir)) logger.warn("fc-working-dir is null or empty.");

			/* Call Process connector: 
			 *   workingDir : compileWorkingDir
			 *   command : rexx compile_cobol 
			 *   params:
			 *      - inputFile. File with source files to compile
			 *      - outputFile. File with result of compilation
			 *   	- executionPath. Executable path
			 *      - sourcePath. Source compile path
			 *    pid 
			 *    
			 *    Parameters example: -inputFile:C:/GRID/1.txt -executionPath:C:/GRIDDATA/sources/ 
			 *    					  -sourcePath:C:/GRIDDATA/exec/ -outputFile:C:/GRID/2.txt
			 * */
			
			/* Process connector parameters for compile COBOL */
			String pcParameters = "[ -inputFile:" + srcFilename + "-outputFile:" + targetFile + 
								  " -executionPath:" + executionPath + " -sourcePath:" + sourcePath + 
								  " -timestamp:" + timestampPGM + "]";
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(compileCobolWorkingDir, rexxCommand,
																			pcParameters,pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			logger.info("******************************************************");
			logger.info("[COMPILECOBOL] End execute compile cobol with pid :: " + pid); 
			logger.info("[COMPILECOBOL] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[COMPILECOBOL] Return Code :: " + exitCode);
			logger.info("[COMPILECOBOL] Exit Status Code :: " + exitStatusCode);
			logger.logTime("[COMPILECOBOL] Time", timeStartSort, System.currentTimeMillis());
			logger.info("******************************************************");

			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				return action.findForward("error");
			}
			else if (exitStatusCode.equalsIgnoreCase("OK")) {
				try {
					/* Get from target file the compilation result */
					String responseCompileCobol = getResponseCompileCobol(targetFile);
					action.getResponse().put("responseCompileCobol", responseCompileCobol);
					return action.findForward("success");
				}
				catch (IOException ioex) {
					logger.error("Error reading target file with command result",ioex);
					action.getResponse().put("exitStatusCode", "KO");
					action.getResponse().put("gridrc", "9981");
					action.getResponse().put("gridrcDesc", "Error reading target file with command result.");
					return action.findForward("error");
				}
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
			logger.error("Error creating source file with source files",ioex);
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
			logger.error("Error processing CompileCobol.", th);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
			action.getResponse().put("gridrcDesc", "Error compiling source COBOL");
			return action.findForward("error");
		}
	}

	/**
	 * Generates a file with source files
	 * 
	 * @param files        : List of FileInfo with all source files.
	 * @param srcFilename  : Source file name 
	 * @return
	 * @throws IOException
	 */
	private void generateCompileCobolFile(FileInfo[] files, String srcFilename) throws IOException {	
		FileWriter file = new FileWriter(srcFilename);
		PrintWriter pw = new PrintWriter(file);
		String eol = System.getProperty("line.separator");
		for (int i=0, iUntil = files.length; i<iUntil; i++)
			pw.write("// " + files[i].getLabel() + " " + files[i].getName() + " " + files[i].getType() + eol);
		pw.close();
	}
	
	private String getResponseCompileCobol(String filename) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(filename));
        String str = "";
        StringBuilder response = new StringBuilder("");
        while ((str = in.readLine()) != null) {
            response.append(str);
        }
        in.close();
        return response.toString();
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}
