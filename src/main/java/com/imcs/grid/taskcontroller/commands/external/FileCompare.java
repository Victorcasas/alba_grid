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
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class FileCompare extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
	
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String workingdir = (String)action.getRequest().get("fc-working-dir");
		String rexxCommand = (String) action.getRequest().get("compareRexx");
		FileInfo[] inputs = (FileInfo[])action.getRequest().get("files");
		String sysin = (String) action.getRequest().get("sysin");		
		String serviceID = (String) action.getRequest().get("idService");	
		
//		String sortRanges = (String)action.getResponse().get("sortRanges");
		
		/* Specific FileCompare parameters */
		String tmpDir = (String)parameters.getProperty("tmpDir");	
		String filecompareCommand = (String)parameters.getProperty("filecompareCommand");
		String sortkeysCommand = (String)parameters.getProperty("sortkeysCommand");
//		String rexxCommand = (String) parameters.getProperty("compareRexx");
		
		/* Filecompare work space defined in taskcontroller.xml */		
		String workspaceSrt = "";
		try {
			workspaceSrt = TaskControllerParam.getInstance().getServiceWorkspaceDir("filecompare") + File.separator;
		} catch (TaskControllerParamException e) {		
			logger.error("Error to load filecompare workspace param. ", e);
		}
		
		logger.info("******************************************************");
		logger.info("[FILECOMPARE] Init execute file compare with pid + " + pid); 
		logger.info("[FILECOMPARE] Parameters :: " + parameters);
		logger.info("******************************************************");
	
		String result = "";
		int rc = 0;
		String rcDesc = "";
		int rcActual = 0;
		String rcDescActual = "";
		String pcParamsSortRanges = "";
		String pcParamsPossibleDiff = "";
		String hasSumFields = "-sumfields:n";
		String unsorted = "-unsorted:n";
		
		/* If SYSIN is empty it is not necessary to get the keys */
		if (!sysin.equals("")) {
			unsorted = "-unsorted:y";
			/* Must be removed the quotes from SYSIN */
			sysin = sysin.substring(1,sysin.length()-1);
			
			/* Only the keys should be obtained when the sentence containing the string SORT FIELDS=( */
			if (sysin.contains("SORT FIELDS=(")) {
				/* Remove comments from SYSIN */
				sysin = removeComments(sysin);
				
				/* Sort work space defined in taskcontroller.xml */
				try {
					String workspaceSort = TaskControllerParam.getInstance().getServiceWorkspaceDir("sort") + File.separator;
					
					/* Generate source file with SYSIN */
					long timestamp = Calendar.getInstance().getTimeInMillis();
					String srcFilename = workspaceSort + "sort" + jcl + "-" + step + "-" + timestamp + ".sysin" ;
					
					/* Name file with ranges */
					String rangesFilename = workspaceSort + "sort" + jcl + "-" + step + "-" + timestamp + ".rank" ;
					
					writeSysinInFile(sysin.replaceAll("\\|"," "),srcFilename);
					
					/* Process connector parameters */
					pcParamsSortRanges += "[" + sortkeysCommand + " -operation:sortkeys -inputFile:" + srcFilename + 
								" -outputFile:" + rangesFilename + "]";
					
					int beginPos = sysin.indexOf("SORT FIELDS=(")+13;
					String aux = sysin.substring(sysin.indexOf("SORT FIELDS=(")+13);
					int endPos = beginPos+aux.indexOf(')');
					String sortFields = sysin.substring(beginPos,endPos);
					if (sortFields.contains("PD")) {
						/* Name file with possible differents */
						String possibleDiffFilename = workspaceSort + "sort" + jcl + "-" + step + "-" + timestamp + ".rank" ;
						
						/* Process connector parameters */
						pcParamsPossibleDiff += "[" + sortkeysCommand + " -operation:possibledifferent -inputFile:"+ srcFilename +
								" -outputFile:" + possibleDiffFilename + "]";
					}
					
				}
				catch (TaskControllerParamException tcex) {
					logger.warn("Could not get work space defined in taskcontroller.xml.",tcex);
				}
				catch(IOException ioex) {
					logger.warn("Could not write SYSIN in the file. The comparison will be without sort keys.",ioex);
				}
			
			}
			else
				logger.info("[SORTKEYS] SYSIN has not SORT fields.");
		}
		else
			logger.info("[SORTKEYS] SYSIN is empty. Is not necessary to get the keys");		
		
		if (sysin.contains("SUM FIELDS=("))
			hasSumFields = "-sumfields:y";
			
		
		/* 	Files for comparison come in pairs (input[i] with input[i+1]) */
		String file1 = null, file2 = null, lenRec, fileType, mod, pcParams, size = "0";
		ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
//		TaskControllerEventEngine eventEngine = TaskControllerEventEngine.getInstance();
		ProcessConnectorBinding connector = null;
		ShellCommandOutput output = null;
		for (int i=0,iHasta=inputs.length;i<iHasta;i=i+2) {
			if (!inputs[i].getLabel().equals(inputs[i+1].getLabel())) 
				throw new TaskControllerException("Labels are different",ErrorType.ERROR);
		
			/* Get file names */
			file1 = inputs[i].getName();
			file2 = inputs[i+1].getName();
			
			/* Get file record lengths */
			lenRec = inputs[i].getRecordLen();
			
			/* Get files type */
			if (inputs[i].getDcb().contains("RECFM=VB"))
				fileType = "-variableRecord:y";
			else
				fileType = "-variableRecord:n";
			
			/* Get file sizes */			
			if ((!UtilString.isNullOrEmpty(inputs[i].getSize())))
				size = inputs[i].getSize();
			else if ((!UtilString.isNullOrEmpty(inputs[i+1].getSize())))
				size = inputs[i+1].getSize();			
			
			mod = (inputs[i].getDisp().equalsIgnoreCase("mod")) ? " - MOD" : "";
				
			try {
				
				//logger.info("::::::::::::::::::::::::: workspaceSrt ::: "+workspaceSrt);
				/* Process connector parameters. Sort keys are added by the process connector */
				pcParams = pcParamsSortRanges + pcParamsPossibleDiff + "[" + filecompareCommand + " {" + file1 + " " + file2 + "} " 
							+ lenRec + " " + fileType + " " + tmpDir + " " + size + " " + inputs[i].getRecords() + " " + workspaceSrt + " "
							+ hasSumFields + " " + unsorted + "]";
				
				/* Call Process connector */
				connector = new ProcessConnectorBinding(workingdir,rexxCommand,pcParams,pid);
				pcMng.addProcess(pid, connector);
				pcMng.startProcess(connector);
				output = ProcessConnectorMng.getInstance().monitor(connector);
				
				rcActual = output.getExitCode();
				rcDescActual = output.getExitCodeDescription();
			}
			catch (ProcessConnectorException pcex) {
				logger.error("Error connecting with Process Connector.", pcex);
				try {
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,file1,file2,"Error in connection with Process Connector.",
							serviceID, Integer.toString(rcActual), rcDescActual);
				} catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
//				eventEngine.callAddResultParallel(jcl,step,file1,file2,"Error in connection with Process Connector.");
				action.getResponse().put("gridrc","9996");
				action.getResponse().put("gridrcDesc","Error connecting with Process Connector");
				return action.findForward("error");
			}
			catch (InterruptedException iex) {
				logger.error("Error monitoring Process Connector.", iex);
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,file1,file2,"Error monitoring Process Connector.",
							serviceID, Integer.toString(rcActual), rcDescActual);
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
//				eventEngine.callAddResultParallel(jcl,step,file1,file2,"Error monitoring Process Connector.");
				action.getResponse().put("gridrc","9986");
				action.getResponse().put("gridrcDesc","Error monitoring Process Connector");
				return action.findForward("error");
			}
			catch (Throwable th) {
				logger.error("Error processing FileCompare.", th);
				try
				{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,file1,file2,"Error processing FileCompare",
							serviceID, Integer.toString(rcActual), rcDescActual);
				}catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
//				eventEngine.callAddResultParallel(jcl,step,file1,file2,"Error processing FileCompare");
				action.getResponse().put("gridrc", "9993");
				action.getResponse().put("gridrcDesc", "Error executing FileCompare service");
				return action.findForward("error");
			}
			
			if (rcActual == 0) {
				result = " EQUALS no-sort" + mod;
				rcDescActual = "Equal files";
			}
			
			else if (rcActual == 1) {
				result = " EQUALS sort" + mod;
				rcDescActual = "Equal files with sorting";
			}
			
			else if (rcActual == 2) {
				result = "EQUALS with sort keys" + mod;
				rcDescActual = "Equal files (only keys)";
			}
			
			else if (rcActual == 3) {
				result = " NOT EQUALS (DIFFERENT SIZE)" + mod;
				rcDescActual = "Not equal files (different size)";
			}
			
			else if (rcActual == 4)  {
				result = " NOT EQUALS" + mod;
				rcDescActual = "Not equal files (equal sizes)";
			}
			
			else if (rcActual == 5) {
				result = " One of two files is empty" + mod;
				rcDescActual = "One of two files is empty";
			}
			
			else if (rcActual == 6) {
				result = "Two files are empty" + mod;
				rcDescActual = "Two files are empty";
			}
			
			else if (rcActual == 7) {
				result = " NOT EQUALS (DIFFERENCE AT PD BYTE SIGN)" + mod;
				rcDescActual = "Not equal files (difference found at PD byte sign)";
			}
			
			else if (rcActual == 9) {
				result = " NOT EQUALS" + mod;
				rcDescActual = "Differents files of equal size (no compared by key)";
			}
			
			else if (rcActual >= 10) {
				result = "Command execution error" + mod;
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,file1,file2,result, serviceID,
							Integer.toString(rcActual), rcDescActual);
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
//				eventEngine.callAddResultParallel(jcl,step,file1,file2,result);
				action.getResponse().put("gridrc",String.valueOf(rcActual));
				action.getResponse().put("gridrcDesc",rcDescActual);
				return action.findForward("error");	
			}
			
			/* If no error the result is inserted into database */
			try{
				TaskControllerToBrokerClient.callAddResultParallel(jcl,step,file1,file2,result, serviceID, Integer.toString(rcActual), rcDescActual);
			}catch (AxisFault e) {
				logger.error("Error adding result parallel :: " + e);
			} catch (InterruptedException e) {
				logger.error("Error adding result parallel :: " + e);
			}
//			eventEngine.callAddResultParallel(jcl,step,file1,file2,result);
			logger.info("Result of the comparison between files " + file1 + " and " + file2 + " :: " + result);
			
			/* The return code will be the largest of all the comparisons */
			if (rcActual > rc) {
				rc = rcActual;
				rcDesc = rcDescActual;
			}
		}
		logger.info("******************************************************");
		logger.info("[FILECOMPARE] End execute file compare with pid + " + pid); 
		logger.info("[FILECOMPARE] Global RC :: " + rc);
		logger.info("[FILECOMPARE] Global RC Description :: " + rcDesc);
		logger.info("******************************************************");
		
		action.getResponse().put("gridrc",String.valueOf(rc));
		action.getResponse().put("gridrcDesc",rcDesc);
		return action.findForward("success");
	}			
	
	private String removeComments(String sysin) {
		String beginPart, restPart, endPart;
		String sysinResult = sysin;
		
		while (sysinResult.contains("--") || sysinResult.contains("/*")) {
        	if (sysinResult.contains("--")) {
        		beginPart = sysinResult.substring(0, sysinResult.indexOf("--"));
        		restPart = sysinResult.substring(sysinResult.indexOf("--"));
        		endPart = restPart.substring(restPart.indexOf("|"));
        		sysinResult = beginPart + endPart;
        	} else if (sysinResult.contains("/*")) {
        		beginPart = sysinResult.substring(0, sysinResult.indexOf("/*"));
        		restPart = sysinResult.substring(sysinResult.indexOf("/*"));
        		endPart = restPart.substring(restPart.indexOf("|"));
        		sysinResult = beginPart + endPart;
        	}
        }
		return sysinResult;
	}
	
	private void writeSysinInFile(String sysin, String filename) throws IOException {
		FileWriter file = new FileWriter(filename);
		PrintWriter pw = new PrintWriter(file);
		pw.write(sysin);
		pw.close();
	}
		
	public FileCompare() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
	
	public void checkInput(Action actions) throws TaskControllerException {}

}
