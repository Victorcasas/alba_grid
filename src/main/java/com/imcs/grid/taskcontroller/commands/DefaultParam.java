package com.imcs.grid.taskcontroller.commands;

public class DefaultParam {
	
	private String key = null;
	private Object value = null;
	
	public DefaultParam(String key, Object value) {
		super();
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
}
