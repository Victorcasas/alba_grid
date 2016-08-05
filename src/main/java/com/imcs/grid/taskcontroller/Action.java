package com.imcs.grid.taskcontroller;

import java.util.Hashtable;

import com.imcs.grid.error.ErrorType;

public class Action {
	
	private Request request = null;
	private Response response = new Response();
	private Hashtable<String,Forward> forwards = null;
	private TaskControllerLog log = null;
	
	public Action() { }
	
	public Request getRequest() 
	{
		return request;
	}
	
	public void setRequest(Request request) 
	{
		this.request = request;
	}
	
	public Response getResponse() 
	{
		return response;
	}
	
	public void setResponse(Response response) 
	{
		this.response = response;
	}
	
	public String toString() 
	{
		return request != null ? request.toString() : "request null" + "\n" + 
				response != null ? response.toString() : "response null";
	}

	public void setForwards(Hashtable<String,Forward> forwards) 
	{
		this.forwards = forwards;
	}
	
	public Forward findForward(String idFor) throws TaskControllerException
	{
		Forward forward = forwards.get(idFor);

		if (forward == null)
			throw new TaskControllerException("Unable to locate forward " + idFor, ErrorType.ERROR);
		
		return forward;			
	}
	
	public TaskControllerLog getLog() 
	{
		return log;
	}
	
	public void setLog(TaskControllerLog log) 
	{
		this.log = log;
	}
}