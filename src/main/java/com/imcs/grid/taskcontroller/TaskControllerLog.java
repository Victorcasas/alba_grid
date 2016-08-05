package com.imcs.grid.taskcontroller;

public class TaskControllerLog {
	
	private StringBuffer buffer = new StringBuffer();
	private String pid = null;
	private long timestamp = 0;
	
	public TaskControllerLog(String pid) {
		this.pid = pid;
		this.timestamp = System.currentTimeMillis();
	}
	
	public String getPid() {
		return pid;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public void write(byte[] bytes) {
		buffer.append(bytes);
	}
	
	public void write(String cadena) {
		buffer.append(cadena);	
	}
	
	public String getTrace() {
		return buffer.toString();
	}
}
