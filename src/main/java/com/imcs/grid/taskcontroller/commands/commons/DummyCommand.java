package com.imcs.grid.taskcontroller.commands.commons;

import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class DummyCommand extends TaskControllerCommand
{

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException
	{
		logger.info("******************************************************");
		logger.info("* [PROCESSING] Init DUMMY COMMAND");
		logger.info("******************************************************");
		long timeStartSleep = System.currentTimeMillis();
		status.setPhaseStatus("terminated");
		logger.info("******************************************************");
		logger.info("* [PROCESSING] End DUMMY COMMAND");
		logger.logTime("* [PROCESSING] Sleep Time", timeStartSleep, System.currentTimeMillis());
		logger.info("******************************************************");
		return action.findForward("success");
	}
	
	public void checkInput(Action actions) throws TaskControllerException {}
	
	public DummyCommand() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
	
	public void deploy() throws TaskControllerException {}
	
	public void undeploy() throws TaskControllerException {}
	
	public void install() throws TaskControllerException {}
}
