package com.imcs.grid.taskcontroller.parser;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.error.GridException;

public class TaskcontrollerParserException extends GridException {

	private static final long serialVersionUID = 1L;

	public TaskcontrollerParserException(String message, ErrorType errorType) {
		super(message, errorType);
	}
	
	public TaskcontrollerParserException(String message,Throwable cause) {
		super(message, cause, ErrorType.ERROR);
	}
	
	public TaskcontrollerParserException(String message,Throwable cause, ErrorType errorType) {
		super(message, cause, errorType);
	}
	
}
