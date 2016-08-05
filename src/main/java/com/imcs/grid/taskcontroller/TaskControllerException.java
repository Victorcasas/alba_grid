package com.imcs.grid.taskcontroller;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.error.GridException;

public class TaskControllerException extends GridException {

	private static final long serialVersionUID = 1L;

	public TaskControllerException(String message, ErrorType errorType) {
		super(message, errorType);
	}
	
	public TaskControllerException(String message,Throwable cause) {
		super(message, cause, ErrorType.ERROR);
	}
	
	public TaskControllerException(String message,Throwable cause, ErrorType errorType) {
		super(message, cause, errorType);
	}
	
}
