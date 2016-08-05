package com.imcs.grid.taskcontroller.parser;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;

public interface ServiceInputParser {
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException;
	public OMElement responseParse(Response input) throws TaskControllerException;

}