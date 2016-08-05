package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.axis2.AxisFault;

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
import com.imcs.grid.util.FileInfo;

public class TranslateToCoSort extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status)
	throws TaskControllerException {
		
		String pid = (String)action.getRequest().get("pid");
		logger.info("[" + pid + "] starting translate to cosort commnad");
		
		String mode = (String)action.getRequest().get("mode");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");			
		String idExecution = (String)action.getRequest().get("idExecution");

		String mvssort = (String)action.getRequest().get("mvssort");	
		String sysin = (String)action.getRequest().get("sysin");
		sysin = sysin.substring(1,sysin.length()-1);
		
		//Replaces the file names because cosort truncates
		FileInfo[] filesIn = (FileInfo[])action.getRequest().get("files");		
		for (int i=0, iHasta=filesIn.length; i<iHasta;i++) {	
			logger.info("name in " + i + " :: " + filesIn[i].getName());
			mvssort = mvssort.replace(filesIn[i].getName(),"fileIn_" + i);
		}
		FileInfo[] filesOut = (FileInfo[])action.getRequest().get("filesOutput");		
		for (int i=0, iHasta=filesOut.length; i<iHasta;i++) {	
			logger.info("name out " + i + " :: " + filesOut[i].getName());
			mvssort = mvssort.replace(filesOut[i].getName(),"fileOut_" + i);
		}
		
		//Create file to translate with mvs2scl cosort command (*.mvs)
		String path_file_gen = parameters.getProperty("path_file_gen");
		File fileSortMvs = new File (path_file_gen + "sort" + System.currentTimeMillis() + ".mvs");	
		try {
			FileOutputStream output = new FileOutputStream(fileSortMvs);
			output.write(mvssort.getBytes());
			byte[] newLine = "\n".getBytes();
			output.write(newLine);
			output.write(sysin.getBytes());
			output.close();
		}
		catch (Throwable th) {
			logger.error("Error to generate file .mvs");
			status.setStatus("error");
			return action.findForward("error");
		}
		//Execute mvs2scl command with process conector
		try {
			String workingdir = (String) action.getRequest().get("fc-working-dir");
			ShellCommandOutput output = executeTranslateToCoSort(pid,workingdir,jcl,step,idExecution,fileSortMvs);
			if (output.getExitStatusCode().equalsIgnoreCase("ko")) {
				logger.error("Error to execute mvs2scl command. Exit Code::" + output.getExitCode());
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Translate to cosort","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Translate to cosort");
				return action.findForward("error");
			}
		}
		catch (ProcessConnectorException ex) {
			logger.error("Error to execute process conector");
			status.setStatus("error");		
			return action.findForward("error");
		}
		
		File fileCoSort = new File(fileSortMvs.getAbsolutePath().replace(".mvs", ".cosort")); 
		action.getResponse().put("fileCoSort", fileCoSort);
		action.getResponse().put("mode", mode);
		status.setStatus("terminated");
		return action.findForward("success");
	}
	
	private ShellCommandOutput executeTranslateToCoSort(String pid, String workingdir, String jcl, 
			String step, String idExecution, File fileMvs) throws ProcessConnectorException {
		
		try {			
			logger.info("[" +  pid + "] calling connectod " + workingdir);
			String nameFileOut = fileMvs.getAbsolutePath().replace(".mvs", ".cosort");
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingdir,
					"mvs2scl " + fileMvs.getAbsolutePath() + " " + nameFileOut,
					"", pid);
			
			ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
			pcMng.addProcess(pid, connector);
			pcMng.startProcess(connector);
			ShellCommandOutput output = pcMng.monitor(connector);
			
			/*Set statistics alebra*/	
			//setStatisticsAlebra(connector,jcl,step,pid,idExecution);
			
			return output;
		}
		catch (Exception ex) {						 					
			throw new ProcessConnectorException("Error when translating to cosort",ex);
		}	
	}
	
	public void checkInput(Action action) throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void deploy() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void install() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void loadConfiguration() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void undeploy() throws TaskControllerException {
		// TODO Auto-generated method stub
	}
}
