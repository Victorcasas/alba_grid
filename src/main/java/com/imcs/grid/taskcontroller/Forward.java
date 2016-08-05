package com.imcs.grid.taskcontroller;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.util.UtilString;

public class Forward {
	
	private String id = null;
	private String toStep = null;
	
	public Forward(String id, String toStep) throws TaskControllerException {
		super();
		if (UtilString.isNullOrEmpty(id))
			throw new TaskControllerException("Unable to create Forward without id", ErrorType.ERROR);
		
		if (UtilString.isNullOrEmpty(toStep)) 
			throw new TaskControllerException("Unable to create Forward without toStep ", ErrorType.ERROR);
		
		this.id = id;
		this.toStep = toStep;
	}
	
	public String getToStep() {
		return toStep;
	}
	
	public void setToStep(String toStep) {
		this.toStep = toStep;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String toString() {
		return "Id: " + id + " toStep: " + toStep;
	}
	
	public boolean equals(Object obj){
		if (obj instanceof Forward)
			return id.equals(((Forward) obj).getId());
		return false;	
	}

	public boolean isGlobal() {
		return toStep.startsWith("global.");
	}
}
