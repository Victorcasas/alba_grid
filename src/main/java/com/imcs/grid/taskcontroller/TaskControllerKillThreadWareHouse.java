package com.imcs.grid.taskcontroller;

import java.util.Hashtable;
import java.util.Map;

import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerKillThreadWareHouse {
	
	private static TaskControllerKillThreadWareHouse instance = null;
	private static Map<String, TaskControllerKillThread> processMonitoring = null;
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerKillThreadWareHouse.class);	
	
	private TaskControllerKillThreadWareHouse() {
		logger.info("TaskControllerKillThreadWareHouse initialized");		
		processMonitoring = new Hashtable<String,TaskControllerKillThread>();
	}
	
	public static TaskControllerKillThreadWareHouse getInstance() {
		if (instance == null)
			instance = new TaskControllerKillThreadWareHouse();
		return instance;
	}
	
	public TaskControllerKillThread getProcessMonitoring(String pid) {
		return processMonitoring.get(pid);
	}
	
	public void setProcessMonitoring(String pid, int time, String jcl) {
		TaskControllerKillThread tckt = new TaskControllerKillThread(time, jcl, pid);
		tckt.start();
		processMonitoring.put(pid, tckt);		
		logger.info("Number of process monitoring: " + processMonitoring.size() );
	}
	
	public void deleteProcessMonitoring(String pid) {
		processMonitoring.remove(pid);
	}
}