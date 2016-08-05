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
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.FileInfo;

public class LaunchOpenCobol extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {

		long timeStartCobol = System.currentTimeMillis();

		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;

		/* Generic executions parameters */
		String pid = (String) action.getRequest().get("pid");
		String jcl = (String) action.getRequest().get("jcl");
		String step = (String) action.getRequest().get("step");
		String mode = (String) action.getRequest().get("mode");
		String statistics = (String) action.getRequest().get("statistics");
		String pgmTimestamp = (String) action.getRequest().get("timestamp");
		String idExecution = (String) action.getRequest().get("idExecution");

		/* File Names */
		FileInfo[] inputs = (FileInfo[]) action.getRequest().get("files");
		FileInfo[] outputs = (FileInfo[]) action.getRequest().get("filesOutput");
		FileInfo[] filesMod = (FileInfo[]) action.getRequest().get("filesMod");

		try {
			logger.info("******************************************************");
			logger.info("[COBOL] Init execute cobol (OpenCOBOL) with pid " + pid);
			logger.info("[COBOL] Parameters :: " + parameters);
			logger.info("******************************************************");

			action.getResponse().put("mode", mode);
			action.getResponse().put("exitStatusCode", exitStatusCode);

			/* Specific Cobol parameters */
			String exeCobol = (String) action.getRequest().get("exe.cobol");
			String exeExtension = parameters.getProperty("exeExtension");
			String exePath = parameters.getProperty("exePath");
			String exeLocation = exePath + File.separator + exeCobol + exeExtension;
			if (statistics.equalsIgnoreCase("true"))
				exePath = parameters.getProperty("exeStatisticsPath");

			/* Cobol workspace defined in taskcontroller.xml */
			String workspaceCbl = TaskControllerParam.getInstance().getServiceWorkspaceDir("cobol") + File.separator;

			/* Generate source file with file names */
			long timestamp = Calendar.getInstance().getTimeInMillis();
			String srcfile = workspaceCbl + "cobol" + jcl + "-" + step + "-" + timestamp + ".cob";
			createCobolSourceFile(inputs, outputs, filesMod, srcfile);

			/* Directory where you run the command */
			String cobolWorkingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(cobolWorkingDir)) logger.warn("fc-working-dir is null or empty.");

			/*
			 * Call Process connector: 
			 * 	 workingDir : cobolWorkingDir 
			 * 	 command : rexx OpenCobol_Pipe 
			 *   params: 
			 *      - file: File name with input, output and mod files 
			 *      - exeFile: File with executable 
			 *      - pgmTimestamp: Process timestamp 
			 *   pid
			 */
			String pcParameters = "[" + srcfile + " " + exeLocation + " " + pgmTimestamp + "]";
			ProcessConnectorBinding connector = new ProcessConnectorBinding(cobolWorkingDir, "rexx OpenCobol_Pipe", 
																			pcParameters, pid);
			ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
			pcMng.addProcess(pid, connector);
			pcMng.startProcess(connector);
			ShellCommandOutput output = pcMng.monitor(connector);
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();

			/* Get Alebra statistics */
			try {
				StatisticsUtils.setAlebraStatistics(connector, jcl, step, idExecution, pid);
			} catch (AxisFault e) {
				logger.error("Error adding Alebra statistics :: " + e);
			} catch (InterruptedException e) {
				logger.error("Error adding Alebra statistics :: " + e);
			}

			logger.info("******************************************************");
			logger.info("[COBOL] End execute cobol (OpenCOBOL) with pid :: " + pid);
			logger.info("[COBOL] Exe Location :: " + exeLocation);
			logger.info("[COBOL] JCL :: " + jcl + " - STEP :: " + step);
			logger.info("[COBOL] Return Code :: " + exitCode);
			logger.info("[COBOL] Exit Status Code :: \"" + exitStatusCode + "\"");
			logger.logTime("[COBOL] Time", timeStartCobol, System.currentTimeMillis());
			logger.info("******************************************************");

			action.getResponse().put("gridrc", String.valueOf(exitCode));
			action.getResponse().put("gridrcDesc", exitCodeDescription);
			action.getResponse().put("exitStatusCode", exitStatusCode);

			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				logger.info("exitStatusCode KO!");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch Open Cobol","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "Error - Launch Open Cobol");
				}
				return action.findForward("error");
			} else if (exitStatusCode.equalsIgnoreCase("OK"))
				return action.findForward("success");
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or "
								+ "KO -> '" + exitStatusCode + "'");
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
					} catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "","ERROR executing cobol (OpenCOBOL)");
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("gridrcDesc","Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		} catch (TaskControllerParamException tcex) {
			logger.error("Error getting parameters used by the service.", tcex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "ERROR executing cobol (OpenCOBOL)");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9998");
			action.getResponse().put("gridrcDesc","An error occurred due to the parameters used by the service.");
			return action.findForward("error");
		} catch (IOException ioex) {
			logger.error("Error creating source file with the filenames", ioex);
			if ((mode != null) && (mode.equalsIgnoreCase("parallel"))){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "ERROR executing cobol (OpenCOBOL)");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9997");
			action.getResponse().put("gridrcDesc","An error occurred due to the source file used by the service.");
			return action.findForward("error");
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "ERROR executing cobol (OpenCOBOL)");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("gridrcDesc","Error connecting with Process Connector");
			return action.findForward("error");
		} catch (NumberFormatException nfex) {
			logger.error("Error getting alebra statistics.", nfex);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "ERROR executing cobol (OpenCOBOL)");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9991");
			action.getResponse().put("gridrcDesc","Error getting alebra statistics.");
			return action.findForward("error");
		} catch (Throwable th) {
			logger.error("Error processing LaunchCblAlebra.", th);
			if (mode != null && mode.equalsIgnoreCase("parallel")){
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","ERROR executing cobol (OpenCOBOL)","","","");
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
			}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl, step, "", "", "ERROR executing cobol (OpenCOBOL)");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9995");
			action.getResponse().put("gridrcDesc","Error executing cobol (OpenCOBOL) service");
			return action.findForward("error");
		}
	}

	public void createCobolSourceFile(FileInfo[] inputs, FileInfo[] outputs, FileInfo[] mods, 
									  String srcFilename) throws IOException {
		FileWriter file = new FileWriter(srcFilename);
		PrintWriter pw = new PrintWriter(file);
		String eol = System.getProperty("line.separator");

		if (inputs != null) {
			for (int i = 0, iHasta = inputs.length; i < iHasta; i++)
				pw.write("//INFILE " + inputs[i].getLabel() + " " + inputs[i].getName() + eol);
		}
		if (outputs != null) {
			for (int i = 0, iHasta = outputs.length; i < iHasta; i++)
				pw.write("//OUTFILE " + outputs[i].getLabel() + " " + outputs[i].getName() + eol);
		}
		if (mods != null) {
			for (int i = 0, iHasta = mods.length; i < iHasta; i++)
				pw.write("//MODFILE " + mods[i].getLabel() + " " + mods[i].getName() + eol);
		}
		pw.flush();
		pw.close();
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