package com.imcs.grid.taskcontroller;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerSessionMng {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerSessionMng.class);
	private static TaskControllerSessionMng instance = null;
	private Map<String, List<String>> sessionRequests = new Hashtable<String, List<String>>();
	
	private TaskControllerSessionMng() {		
	}
	
	public static TaskControllerSessionMng getInstance() 
	{
		if (instance == null) 
			instance = new TaskControllerSessionMng();
		return instance;
	}
	
	public void createSession(String id) 
	{
		sessionRequests.put(id, new ArrayList<String>(10));
		logger.info("Created session :: " + id);
	}
	
	public void dropSession(String id) 
	{
		sessionRequests.remove(id);		
		logger.info("Dropped session :: " + id);
	}
	
	public int addRequest(String idSession, String pid) 
	{
		try {
			if (!sessionRequests.containsKey(idSession)) 
				throw new IllegalStateException("Unable to add " + pid + " to session " +  idSession + " create before");
			
			sessionRequests.get(idSession).add(pid);
			return sessionRequests.get(idSession).size();
		}
		finally {
			logger.debug("Added request - Session :: " + idSession + " ; Pid ::" + pid);
		}
	}
	
	public List<String> getSessionPids(String idSession) 
	{
		return sessionRequests.get(idSession);
	}

	public boolean containsSession(String activeSessionId) 
	{
		return sessionRequests.containsKey(activeSessionId);
	}
}