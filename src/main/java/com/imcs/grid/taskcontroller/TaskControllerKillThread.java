package com.imcs.grid.taskcontroller;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axis2.addressing.EndpointReference;

import com.imcs.grid.commons.ws.Client;
import com.imcs.grid.taskcontroller.client.TaskControllerToTopologyClient;
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerKillThread extends Thread{

	private boolean refresh = false;
	private int time;
	private String jcl = null;
	private String pid = null;
	private static boolean stop = false;
	private static GridLog log = GridLogFactory.getInstance().getLog(TaskControllerKillThread.class);	
	
	public TaskControllerKillThread(int time, String jcl, String pid) {
		log.info("## Created TaskControllerKill THREAD " + time + " : " + jcl + " : " + pid);		
		this.time = time;
		this.jcl = jcl;
		this.pid = pid;
	}

	public void run() {
		if (time > 1000)
		while(!stop && time != 0) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				log.error("Time to wait in taskcontrollerkill", e);
			}
			
			if (refresh){
				log.info("## taskcontroller - refreshing time");
				refresh = false;				
			} else {				
				time = time - 1000;
				log.info("## TaskController --> time " + time );
			}
		}
		if (!stop) {
			sendBrokerCloseSession();
		}
		deletingThread();
		
		try {
				this.finalize();
			} catch (Throwable e) {
				log.error("Error to finalize thread "+getName(), e);
			}
		return;	
	}	

	private void deletingThread() {
		log.info("## TaskController --> deletingThread " + pid);
		TaskControllerKillThreadWareHouse.getInstance().deleteProcessMonitoring(pid);		
	}
	public void stopThread() {
		log.info("## STOPPING THREAD pid(" + pid + ")");
		stop = true;
	}

	private void sendBrokerCloseSession() {
		log.warn("## TaskController --> KILLING SESSION [" + jcl + "]");
        try {		
			String location = TaskControllerToTopologyClient.getRootLocation();
			EndpointReference targetEPR = new EndpointReference("http://" + location + "/ws/services/broker");
	
	        OMFactory fac = OMAbstractFactory.getOMFactory();
	        OMNamespace omNs = fac.createOMNamespace("", "");
	
	        OMElement closeSession = fac.createOMElement("closeSession", omNs);
	        OMElement gridjob = fac.createOMElement("gridjob", omNs);
	        OMElement mvsinfo = fac.createOMElement("mvsinfo", omNs);
	        mvsinfo.addAttribute("jcl", jcl, omNs);
	        gridjob.addChild(mvsinfo);
	        closeSession.addChild(gridjob);
	        String action =  "urn:" + TaskcontrollerNS.CLOSESESSION.getValue();
        	Client.clientBlocking(closeSession, targetEPR, false, action);
		} catch (Exception e) {
			log.error("Doing closeSession", e);
		}
	}

	public void setTime(int time) {
		log.info("## TaskController -- new time [" + time + "]");
		refresh = true;
		this.time = time;
	}
}