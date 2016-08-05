package com.imcs.grid.taskcontroller;

public class Mapping {
	
	private String from = null;
	private String to = null;
	
	public Mapping(String from, String to) {
		super();
		this.from = from;
		this.to = to;
	}
	
	public String getFrom() {
		return from;
	}

	public String getTo() {
		return to;
	}
	
	public String toString() {
		return "From: " + from + " to: " + to;
	}
}
