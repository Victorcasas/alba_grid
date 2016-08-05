package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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

public class LaunchCoSort extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
					
		String pid = (String)action.getRequest().get("pid");
		logger.info("[" + pid + "] starting sort commnad");

		String mode = (String)action.getRequest().get("mode");
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");			
		String idExecution = (String)action.getRequest().get("idExecution");

		File fileCosort = (File)action.getRequest().get("fileCoSort");
		
		List<String> params = new ArrayList<String>();
		int i=1;
		String param = "";
		logger.info("Params - COSORT");
		while (parameters.getProperty("paramCOSort"+i)!=null) {
			param = parameters.getProperty("paramCOSort"+i); 
			params.add(param);
			logger.info("paramCOSort" + i + "::" + param);
			i++;
		}
		logger.info("Finish retrieving sort params");

		logger.info("******************************************************");
		logger.info("[PROCESSING] Init SORT " + fileCosort.getName());
		logger.info("******************************************************");
			
		try { 
			String workingdir = (String) action.getRequest().get("fc-working-dir");
			ShellCommandOutput output = executeCOSort(pid,workingdir,jcl,step,idExecution,fileCosort);
				
			if (output.getExitStatusCode().equalsIgnoreCase("ko")) {
				logger.error("Error sort. Exit Code::" + output.getExitCode());
				if ((mode != null) && (mode.equalsIgnoreCase("parallel"))) {
					try{
						TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error - Launch COSort","","","");
					}
					catch (AxisFault e) {
						logger.error("Error adding result parallel :: " + e);
					} catch (InterruptedException e) {
						logger.error("Error adding result parallel :: " + e);
					}
				}
//					TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error - Launch COSort");
				return action.findForward("error");
			}
			logger.info("******************************************************");
			logger.info("[PROCESSING] End SORT " + fileCosort.getName());
			logger.info("******************************************************");

			action.getResponse().put("mode", mode);
			status.setStatus("terminated");
			return action.findForward("success");
		}
		catch (ProcessConnectorException ex) {
			logger.error("Error processing COSORT", ex);
			status.setMessageDescription("Error processing COSORT");			
			if ((mode!=null) && (mode.equalsIgnoreCase("parallel"))) {
				try{
					TaskControllerToBrokerClient.callAddResultParallel(jcl,step,"","","Error processing Launch CoSort","","","");
				}
				catch (AxisFault e) {
					logger.error("Error adding result parallel :: " + e);
				} catch (InterruptedException e) {
					logger.error("Error adding result parallel :: " + e);
				}
//				TaskControllerEventEngine.getInstance().callAddResultParallel(jcl,step,"","","Error processing Launch CoSort");
			}			
			return action.findForward("error");
		}
	}

	private ShellCommandOutput executeCOSort(String pid, String workingdir, String jcl, 
		String step, String idExecution, File fileCosort) throws ProcessConnectorException {
	
		try {			
			logger.info("[" +  pid + "] calling connectod " + workingdir);
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingdir,
					"sortcl " + fileCosort.getAbsolutePath(),
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
	
	

	public void checkInput(Action actions) throws TaskControllerException {
		// TODO Auto-generated method s
	}

	public void loadConfiguration() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void install() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void deploy() throws TaskControllerException {
		// TODO Auto-generated method stub
	}

	public void undeploy() throws TaskControllerException {
		// TODO Auto-generated method stub
	}
}