package com.imcs.grid.taskcontroller;

public class GlobalExecution {
	
	private Command command = null;
	private String id = null;
	
	public GlobalExecution(String id, Command command) throws TaskControllerException {
		this.command = command;
		this.id = id;
	}

	public Command getCommand() {
		return command;
	}
	
	public String toString() {
		return "Workflow.GlobalExecution Id: " + id + " commandClass: " + command ;
	}

	public String getId() {
		return id;
	}
}
