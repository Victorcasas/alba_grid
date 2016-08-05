package com.imcs.grid.taskcontroller.commands.commons;

import java.util.Properties;

import org.apache.axis2.AxisFault;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.UtilString;

public class ReportSuccess extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.info("************************************************ ");
		logger.info("**************** ReportSuccess ***************** ");
		logger.info("************************************************ ");		
		
		status.setStatus("terminated");
		
		String jcl = (String)action.getRequest().get("jcl");
		String step = (String)action.getRequest().get("step");
		String idService = (String)action.getRequest().get("idService");
		String rc = (String)action.getResponse().get("gridrc");
		String rcDescription = (String)action.getResponse().get("gridrcDesc");
		
		if (UtilString.isNullOrEmpty(rc)) rc = "0";
		if (UtilString.isNullOrEmpty(rcDescription)) rcDescription = "";
		
		status.setMessageDescription(rcDescription);
		
		String gridpid = status.getExecutionThread().getPid();
		
		
			try {
				TaskControllerToBrokerClient.callAddServiceExecution(gridpid, jcl, step, 
						Node.getMyself().getLocation(), idService, "success", rc, rcDescription);
			} catch (AxisFault a) {
				logger.error("Error adding service execution :: " + gridpid + " :: "+jcl+" :: "+step+" :: "+Node.getMyself().getLocation()
						+" :: "+idService+" :: success :: "+ rc + " :: " + rcDescription, a);
			} catch (InterruptedException e) {
				logger.error("Error adding service execution :: " + gridpid + " :: "+jcl+" :: "+step+" :: "+Node.getMyself().getLocation()
						+" :: "+idService+" :: success :: "+ rc + " :: " + rcDescription, e);
			}
		
		return null;
	}

	public void checkInput(Action actions) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}