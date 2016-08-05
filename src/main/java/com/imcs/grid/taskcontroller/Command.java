package com.imcs.grid.taskcontroller;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.util.UtilString;

public class Command {
	
	private String id = null;
	private String commandClass = null;
	
	public Command(String id, String commandClass) throws TaskControllerException {
		super();
		if (UtilString.isNullOrEmpty(id))
			throw new TaskControllerException("Unable to create command without id", ErrorType.ERROR);
		
		if (UtilString.isNullOrEmpty(commandClass)) 
			throw new TaskControllerException("Unable to create command without command class", ErrorType.ERROR);
	
		this.id = id;
		this.commandClass = commandClass;
	}
	
	public String getCommandClass() {
		return commandClass;
	}

	public void setCommandClass(String commandClass) {
		this.commandClass = commandClass;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void process(Action action) {
		
	}
	
	public String toString() {
		return "Workflow.command Id: " + id + " commandClass: " + commandClass ;
	}
}
