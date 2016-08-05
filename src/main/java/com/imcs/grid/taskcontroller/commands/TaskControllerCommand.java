package com.imcs.grid.taskcontroller.commands;

import com.imcs.grid.util.log.GridLog;

import com.imcs.grid.util.log.GridLogFactory;

public abstract class TaskControllerCommand implements TaskControllerProcessor, TaskControllerSession, PluggableCommand {
	
	private boolean stop = false;
	
	protected GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerCommand.class);
	
	public boolean isStop() {
		return stop;
	}
	public void setStop(boolean stop) {
		this.stop = stop;
	}
}