package com.imcs.grid.taskcontroller;

import java.util.ArrayList;
import java.util.List;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.util.UtilString;

public class Service {
	
	private String id = null;
	private String desc = null;
	private String summary = null;
	private String inputParserClass = null;
	private List<Step> steps = null;

	public Service(String id) throws Exception {
		if (UtilString.isNullOrEmpty(id))
			throw new TaskControllerException("Unable to create service without ID check the configuration file",ErrorType.ERROR);
		
		this.id=id;		
		desc = "No description available";
		steps = new ArrayList<Step>(5);
	}
	
	public Service(String id, String desc, String summary) throws Exception {
		if (UtilString.isNullOrEmpty(id))
			throw new TaskControllerException("Unable to create service without ID check the configuration file",ErrorType.ERROR);
		
		this.id=id;		
		this.desc = desc;
		this.summary = summary;
		steps = new ArrayList<Step>(5);
	}
	
	public String getInputParserClass() {
		return inputParserClass;
	}

	public void setInputParserClass(String inputParserClass) {
		this.inputParserClass = inputParserClass;
	}

	public String getId() {
		return id;
	}

	public String getDescription() {
		return desc;
	}
	
	public String getSummary() {
		return summary;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public void addStep(Step step) {
		steps.add(step);
	}
	
	public int getNumberSteps() {
		return steps.size();
	}
	
	public Step getStep(int item) {
		return steps.get(item);
	}
	
	public String toString() {
		String aux = "id: " + id + " " +(UtilString.isNullOrEmpty(inputParserClass) ? 
												("inputparser: " + inputParserClass) : "");
		for (Step step : steps) {
			aux += "\n\t" + step.toString();
		}
		return aux+"\n";
	}

	public Step getNextStep(Forward forward) throws TaskControllerException {
		for (Step step : steps) {
			if (forward.getToStep().equals(step.getId()))
				return step;
		}
		throw new TaskControllerException("No step avalaible for forward " + forward ,ErrorType.ERROR);
	}
}
