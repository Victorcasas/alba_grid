package com.imcs.grid.taskcontroller;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.util.UtilString;

public class Step {
	
	private String id = null;
	private Command command = null;
	private Hashtable<String,Forward> forwards = null;
	private List<Mapping> mappings = null;
	private Properties parameters = null;
	
	public Step(String id, Command command) throws TaskControllerException {
		super();
		
		if (UtilString.isNullOrEmpty(id))
			throw new TaskControllerException("Unable to create step without id", ErrorType.ERROR);
		
		if (command == null) 
			throw new TaskControllerException("Unable to create step without command", ErrorType.ERROR);
		
		this.id = id;
		this.command = command;
		this.forwards = new Hashtable<String,Forward>(3,0.2f);
		mappings = new ArrayList<Mapping>(5);
	}
	
	public Command getCommand() {
		return command;
	}
	
	public String getId() {
		return id;
	}
	
	public String toString() {
		StringBuilder aux =  new StringBuilder("Id: " + id + "\n\tCommand: " + command.toString() + "\n");
		aux.append("\tForwards: " + forwards + "\n");
		aux.append("\tMappings: " + mappings);
		return aux.toString();
	}

	public Hashtable<String, Forward> getForwards() {
		return forwards;
	}

	public List<Mapping> getMappings() {
		return mappings;
	}
	
	public void setParameter(String key, String value) {
		if (parameters == null) {
			parameters = new Properties();
		}
		parameters.setProperty(key,value);
	}
	
	public String getParameter(String key) {
		return parameters.getProperty(key);
	}

	public Properties getParameters() {
		return parameters;
	}
}