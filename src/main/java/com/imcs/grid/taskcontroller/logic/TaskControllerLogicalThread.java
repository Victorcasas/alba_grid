package com.imcs.grid.taskcontroller.logic;

import org.apache.axis2.AxisFault;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.client.TaskControllerToTopologyClient;
import com.imcs.grid.taskcontroller.pc.ProcessConnectorManager;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;


/**
 * Launched by TaskControllerLogical, controls periodically that the Process Connector is up and working,
 * and if not, tries to start it up again. 
 * 
 * @author jja 
 * @since 2013-01-08
 * @version Last update 2013-01-16
 * 
 */
public class TaskControllerLogicalThread extends Thread {
	
	private static final GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerLogicalThread.class);	
	private static final GridConfiguration conf = GridConfiguration.getDefault();	
		
	private final ProcessConnectorManager processConnector;	
	private final String mdsAlertBase;
	private boolean continueThread = true;

	public static int s_nThisIPPort;
	private String m_myIP;
		
	/**
	 * Public Constructor. 
	 * Initialize the Process Connector Manager using the parameterized port range. 
	 */
	public TaskControllerLogicalThread() {
		logger.info("Initializing TaskController logical thread...");
		processConnector = ProcessConnectorManager.getInstance();		
		mdsAlertBase = conf.getParameter( ConfigurationParam.MY_NAME)+" ("+m_myIP+":"+s_nThisIPPort+"): ";		
	}
	
	/**
	 * Public Constructor. 
	 * Initialize the Process Connector Manager using the specific port. 
	 */
	public TaskControllerLogicalThread(int port) {
		this();
		processConnector.setPort(port);
		processConnector.setPortMax(port);
	}

	/**
	 * Start Process Connector and monitor it.
	 */
	public void run() {
		if (!processConnector.isStarted()) {
			startProcessConnector();
			checkProcessConnectorStatus(); // check anyway to try restart if can' be started.
		} else { 
			// TODO ¿Send a exception?¿Implements a restart from monitor (web)?
			logger.warn("Process Connector already started.");
		}
	}
	
	/**
	 * Start Process Connector if not already started and notify it status.
	 */
	private void startProcessConnector() {
		processConnector.start();
		
		if (!processConnector.isStarted()) {
			notifyPCNoStarted();
		} else {
			notifyPCUp();
		}
	}
	
	/**
	 * Update the Process Connector status (up or down) and notify it to root and itself.
	 * @param status true if PC is up, otherwise false
	 */
	private void updatePCStatus(boolean status) {
		ProcessConnectorBinding.setStartPC( status );
		String statusRoot = status ? "ok" : "error";
		String statusMyself = status ? "PCResponse" : "PCTimeout";
		try	{
			TaskControllerToBrokerClient.notifyStatusPCToRoot(statusRoot);
			TaskControllerToTopologyClient.changeState(statusMyself);
			
		} catch ( AxisFault e)	{
			logger.error( "Unable to notify PC is " + statusRoot + ". From now on this node state can be inconsistent.", e);
		} catch( Exception e) {
			logger.error( "Serious irrecoverable error. From now on this node state can be inconsistent. Please inform your dealer.", e);
		} 
	}
	
	/**
	 * Loop to check the Process Connector status (up or down) every ping interval time.
	 */
	private void checkProcessConnectorStatus() {
		logger.info("Begin thread to check the status of the process connector.");
		
		int pingsDown = 0;
		boolean pcStatusIsDown = false;
		
		// Parametrize this variable
		final int retriesBeforeStartPC = 2;
		
		do {
//			processConnector.getPingThread().start();
			try {
				Thread.sleep(processConnector.getPING_INTERVAL());
			} catch (InterruptedException e) {
				logger.error("Error sleeping during the ping interval.", e);
			}
			
			processConnector.ping(); // Replaces a pingThread

			// PC is now down or not response for first time
			if (!processConnector.isStarted() && !pcStatusIsDown) {
				logger.warn(String.format("PROCESS CONNECTOR IS POSSIBLY DOWN. Ping retries %d of %d.", pingsDown, retriesBeforeStartPC));
				pingsDown++;
				pcStatusIsDown = true;
				notifyPCPossiblyDown();

			} else if (pcStatusIsDown) {

				if (++pingsDown == retriesBeforeStartPC) {
					logger.error("PROCESS CONNECTOR IS DOWN. Restarting Process Connector...");
					startProcessConnector();
					if (processConnector.isStarted()) {
						logger.info( "PROCESS CONNECTOR IS UP AGAIN.");
						pcStatusIsDown = false;
						pingsDown = 0;
					} else {
						logger.error("PROCESS CONNECTOR CONTINUE DOWN. Process Connector restarted has failed.");
						pingsDown--; // To retry PC starting in the next ping
					}
				} else {
					logger.warn(String.format("PROCESS CONNECTOR IS POSSIBLY DOWN. Ping retries %d of %d.", pingsDown, retriesBeforeStartPC));
				}
			}
		} while (continueThread);
	}
	
	/**
	 * Notify that Process Connector is possibly down.
	 * Update the PC status and write a alert.
	 * @see {@link #updatePCStatus(boolean)}
	 */
	private void notifyPCPossiblyDown() {
		MdsProvider.writeAlert(mdsAlertBase + "Process Connector communication is broken (possibly down).","warnmsg_" + System.currentTimeMillis());
		updatePCStatus(false);
	}
	
	/**
	 * Notify that Process Connector is up.
	 * Update the PC status and write a alert.
	 * @see {@link #updatePCStatus(boolean)}
	 */
	private void notifyPCUp() {
		MdsProvider.writeAlert( mdsAlertBase + "Process Connector communication is up.","infomsg_" + System.currentTimeMillis());
		updatePCStatus(true);
	}
	
	/**
	 * Notify that Process Connector is down.
	 * Update the PC status and write a alert.
	 * @see {@link #updatePCStatus(boolean)}
	 */
	private void notifyPCNoStarted() {
		MdsProvider.writeAlert(mdsAlertBase + " PROCESS CONNECTOR IS DOWN.","errormsg_" + System.currentTimeMillis());
		updatePCStatus(false);
	}
	
	/**
	 * Stop the check PC status loop. Using in shutdown node. 
	 * @see {@link #checkProcessConnectorStatus()} 
	 */
	public void stopThread() { 
		continueThread = false;
		try {
			this.finalize();
		} catch (Throwable e) {			
		}
	}
}
