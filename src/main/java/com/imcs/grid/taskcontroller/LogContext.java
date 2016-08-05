package com.imcs.grid.taskcontroller;

import java.util.Hashtable;
import java.util.Map;

public class LogContext {
	
	private Map<String, TaskControllerLog> logs = new Hashtable<String, TaskControllerLog>();
	
	protected void addLog(String id,TaskControllerLog log) {
		logs.put(id,log);
	}

	public TaskControllerLog getLog(String id) {
		return logs.get(id);
	}

	public TaskControllerLog createLog(String pid) {
		TaskControllerLog log = new TaskControllerLog(pid);
		this.addLog(pid,log);
		return log;
	}
}
