package com.imcs.grid.taskcontroller;


public class JobState {
	
	private String status = null;
	private String messageDescription = null;
	private TaskControllerThread executionThread = null;
	private String logResult = null;
	private String phaseStatus = null;
	
	public JobState(String status) {
		super();
		this.status = status;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getMessageDescription() {
		return messageDescription;
	}

	public void setMessageDescription(String messageDescription) {
		this.messageDescription = messageDescription;
	}

	public String getPhaseStatus() {
		return phaseStatus;
	}

	public void setPhaseStatus(String phaseStatus) {
		this.phaseStatus = phaseStatus;
		//executionThread.changePhaseStatus(phaseStatus);
	}

	public TaskControllerThread getExecutionThread() {
		return executionThread;
	}

	public void setExecutionThread(TaskControllerThread executionThread) {
		this.executionThread = executionThread;
	}
		
	public String getLogResult() {
		return logResult;
	}
	
	public void setLogResult(String messageCmd) {
		this.logResult = messageCmd; 
	}	
}