package com.imcs.grid.taskcontroller.commands;

import com.imcs.grid.taskcontroller.TaskControllerException;

public interface PluggableCommand {
	
	public void loadConfiguration() throws TaskControllerException;
	public void install() throws TaskControllerException;
	public void deploy() throws TaskControllerException;
	public void undeploy() throws TaskControllerException;
	
}