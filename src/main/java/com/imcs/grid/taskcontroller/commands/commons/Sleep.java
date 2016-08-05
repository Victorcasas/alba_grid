package com.imcs.grid.taskcontroller.commands.commons;

import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class Sleep extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		String millis = (String)action.getRequest().get("millis");
		logger.info("XMillis in sleep " + millis);
		int sleep = Integer.parseInt(millis);
		try {
			logger.info("******************************************************");
			logger.info("* [PROCESSING] Init SLEEP " + sleep);
			logger.info("******************************************************");
			long timeStartSleep = System.currentTimeMillis();
			status.setPhaseStatus("Sleeping " + sleep + " ms");
			Thread.sleep(sleep);
			status.setPhaseStatus("terminated");
			logger.info("******************************************************");
			logger.info("* [PROCESSING] End SLEEP");
			logger.logTime("* [PROCESSING] Sleep Time", timeStartSleep, System.currentTimeMillis());
			logger.info("******************************************************");
			
		} catch (InterruptedException e) {
			logger.error("Thread interrupted.", e);
			action.getResponse().put("errormessage", "Problem with sleep.");
			return action.findForward("error");
		}
		return action.findForward("success");
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}
	
	public Sleep() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
}

