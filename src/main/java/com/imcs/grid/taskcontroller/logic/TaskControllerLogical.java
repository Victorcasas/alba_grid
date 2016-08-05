/**
 * 
 */
package com.imcs.grid.taskcontroller.logic;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axis2.AxisFault;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;

import com.imcs.grid.cache.CachePolicy;
import com.imcs.grid.cache.GridCache;
import com.imcs.grid.cache.GridCacheFactory;
import com.imcs.grid.commons.TimeFormat;
import com.imcs.grid.commons.management.alerts.Alerts;
import com.imcs.grid.commons.management.logs.MainLogs;
import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.error.KernelException;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Service;
import com.imcs.grid.taskcontroller.SrvMng;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.TaskControllerSessionMng;
import com.imcs.grid.taskcontroller.TaskControllerThread;
import com.imcs.grid.taskcontroller.TaskControllerWareHouse;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.client.TaskControllerToTopologyClient;
import com.imcs.grid.taskcontroller.host2grid.ParserXML;
import com.imcs.grid.taskcontroller.messages.TaskControllerCreateMessages;
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;
import com.imcs.grid.taskcontroller.pc.ProcessConnectorManager;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.FileOperations;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

/**This class is intended to be the logical support to all services provided by the Task Controller.
 * Among other tasks,
 * -checks every now and then the status of the Process Connector, starting it up if down.
 * -performs the replication of a DB.   
 */

public class TaskControllerLogical {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerLogical.class);
	
	private TaskControllerLogicalThread m_theThread = null;
	private static TaskControllerLogical instance = null;
	
	private String activeSessionId = null;
	private long finishedProcessWait = 3000000L;
	private TaskControllerSessionMng sessionMng = null;
	private boolean executingProcess = false;
	
	private static final String TASKCONTROLLER_TIME_FORMAT = "{0} hh {1} mm {2} ss";
	private static final Format formatter = new SimpleDateFormat("HH:mm - dd/MM/yyyy");
	
	public static TaskControllerLogical getInstance() {
		if (instance == null)
			instance = new TaskControllerLogical();
		return instance;
	}
	
	public void stopModule() throws Throwable {
		instance = null;
		m_theThread.stopThread();
		m_theThread = null;
	}	
	
	private TaskControllerLogical() {
		try {
			try {
				finishedProcessWait = GridConfiguration.getDefault().getParamAsLong(ConfigurationParam.TC_FINISHED_PROCESS_WAIT) * 1000;
			} catch (Exception e){
				logger.error("Error getting finished process delete wait time parameter to taskcontroller.xml", e);
			}			
			initServicesManagement();
			initMainLogsManagement();
			initAlertsManagement();
			sessionMng = TaskControllerSessionMng.getInstance();
			initTaskcontrollerLogicalThread();
		} catch (Throwable tcex) {
			logger.info("DEBUG:: ERROR TaskControllerLogical");
			logger.error(tcex.getMessage(), tcex);
			System.exit(0);
		}
	}
	
	private void initServicesManagement() throws TaskControllerException {
		SrvMng srvMng = SrvMng.getDefault();
		String file = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_SERVICE_FILE);
		logger.info(ConfigurationParam.TC_SERVICE_FILE + " : " + file);
		srvMng.load(file);
	}
	
	private void initMainLogsManagement() {
		MainLogs.initManagementLogs();
	}

	private void initAlertsManagement() {
		Alerts alertMng = new Alerts();
		alertMng.initManagementAlerts();
	}
	
	private void initTaskcontrollerLogicalThread() {
		logger.info("PC startup initializing...");
		m_theThread = new TaskControllerLogicalThread();
		m_theThread.start();
	}
	
	public String performTask(OMElement gridJobMessage)	{
		String pid = "ERROR-"+gridJobMessage.getAttributeValue(new QName("pid"));
		if (!executingProcess) {
			logger.debug("Message receive --> " + gridJobMessage);
			String service = gridJobMessage.getAttributeValue(new QName("", TaskcontrollerNS.OPERATION.getValue()));
			pid = "pgt:" + service + System.currentTimeMillis();
			
			String pidFromBroker = gridJobMessage.getAttributeValue(new QName("pid"));
			activeSessionId = gridJobMessage.getAttributeValue(new QName("sessionid"));
			String deliverTo = gridJobMessage.getAttributeValue(new QName("deliverTo"));
			String mode = gridJobMessage.getAttributeValue(new QName("mode"));
			if (!UtilString.isNullOrEmpty(pidFromBroker)) {
				logger.debug("Pid from Root :: " + pidFromBroker);
				pid = pidFromBroker;
			}
			if (!sessionMng.containsSession(activeSessionId))
				sessionMng.createSession(activeSessionId);
	
			sessionMng.addRequest(activeSessionId, pid);
			TaskControllerThread tct = new TaskControllerThread(pid, activeSessionId, service, mode, new Object[] {gridJobMessage});
			if (!UtilString.isNullOrEmpty(deliverTo))
				tct.setDeliverTo(deliverTo);
			tct.start();
			executingProcess = true;
			TaskControllerWareHouse.getInstance().setJobs(pid, tct, gridJobMessage);
		} else {
			logger.warn("Node is executing a process");
		}
		return pid;
	}
	
	public List<OMElement> getAllSubmittedMessages() {
		return TaskControllerWareHouse.getInstance().getAllSubmittedMessages();
	}
	
	public OMElement getSubmittedMessage(String pid) throws TaskControllerException	{
		return TaskControllerWareHouse.getInstance().getSubmittedMessage(pid);
	}
	
	public OMElement allocate(String service, OMElement ome) throws Throwable {
		OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));	
    	String jcl = null;
    	String step = null;
    	if (mvsInfo != null) {
    		jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
    		step = mvsInfo.getAttributeValue(new QName("", "step"));
    	}
    	
    	OMElement pgm = ome.getFirstChildWithName(new QName("", "pgm"));
    	if (pgm == null)
    		pgm = ome.getFirstChildWithName(new QName("", "pgm"));
    	if (pgm == null)
    		throw new TaskControllerException("Unable to allocate, PGM not received",ErrorType.WARNING);
    	
    	String id = pgm.getAttributeValue(new QName("", "id"));
    	String pid = pgm.getAttributeValue(new QName("", "pid"));
    	if (UtilString.isNullOrEmpty(pid))
    		pid = pgm.getAttributeValue(new QName("", "pid"));
    	if (UtilString.isNullOrEmpty(pid))
    		throw new TaskControllerException("Unable to allocate, pid not received",ErrorType.WARNING);
    		
    	logger.debug("Allocate --> jcl :: " + jcl + "| step :: " + step + "| id :: " + id + "| pid ::" + pid);
    	
    	TaskControllerThread tct = TaskControllerWareHouse.getInstance().getTaskControllerThread(pid);
    	
    	if (tct == null) {
    		logger.warn("PID not found :: " + pid);
    		return TaskControllerCreateMessages.getOMElementResponse(service, jcl, step, id, "00:00 - 00/00/0000", 
    									"0", "error", "PID not found", "", null, null);
    	}
//    	String descJob = null;
//		if (!(UtilString.isNullOrEmpty(jcl))) {
//			descJob = jcl; 
//			if (!UtilString.isNullOrEmpty(step))
//			 descJob += " -- " + step;
//		}
		if (activeSessionId == null) {
			logger.error("No activeSessionId in allocate");
			throw new TaskControllerException("No activeSessionId in allocate",ErrorType.ERROR);
		}
    	tct = new TaskControllerThread(pid,activeSessionId, service,null, new Object[] {ome});
		tct.start();
		TaskControllerWareHouse.getInstance().setJobs(pid, tct, ome);
		
		String status = tct.getLive();
		logger.debug("status :: " + status);
    	Date date = tct.getDate();
    	Date now = new Date();
    	long inter = now.getTime() - date.getTime();
    	String age = TimeFormat.calcHMS(inter,TASKCONTROLLER_TIME_FORMAT);
    	String dateFormatted = formatter.format(date);
    	    	
    	return TaskControllerCreateMessages.getOMElementResponse(service, jcl, step, id, dateFormatted, age, status, "",tct.getLogResult(), null, tct.getResult());
	}
	
	public OMElement monitor(OMElement ome, String totalTime, String serviceTime) {
		OMElement runningReturn = null;
		int sleepTime = Integer.parseInt(serviceTime);
		int totalTimeInt = Integer.parseInt(totalTime);
		
    	if (sleepTime > totalTimeInt) {
    		throw new IllegalArgumentException("Time to sleep is greater than total time");
    	}
		
		long initTimeMs = System.currentTimeMillis();
		long currentTimeMs = System.currentTimeMillis();
		long t = currentTimeMs - initTimeMs;
		
		while (t < totalTimeInt) {
			String namespace = "";
			if (ome.getNamespace() != null)
				namespace = ome.getNamespace().getNamespaceURI();
			OMElement mvsInfo = ome.getFirstChildWithName(new QName(namespace, "mvsinfo"));
	    	String jcl = null;
	    	String step = null;
	    	if (mvsInfo != null) {
	    		jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
	    		step = mvsInfo.getAttributeValue(new QName("", "step"));
	    		logger.debug("Monitor --> jcl :: " + jcl + "; step :: " + step);
	    	}
	    	
	    	OMElement pgm = ome.getFirstChildWithName(new QName(namespace, "pgm"));
	    	if (pgm == null) 
	    		throw new IllegalArgumentException("Not pgm received");
	    	String id = pgm.getAttributeValue(new QName("", "id"));
	    	String pid = pgm.getAttributeValue(new QName("", "pid"));
	
	    	if (UtilString.isNullOrEmpty(pid))
	    		pid = pgm.getAttributeValue(new QName("pid"));
	    	
	    	if (UtilString.isNullOrEmpty(pid))
	    		throw new IllegalArgumentException("Not pid received");
	    	
	    	TaskControllerThread tct = TaskControllerWareHouse.getInstance().getTaskControllerThread(pid);
	   
	    	if (tct == null) {
	    		logger.warn("PID not found :: " + pid);
	    		return TaskControllerCreateMessages.getOMElementResponse("monitor", jcl, step, id, "00:00 - 00/00/0000", 
	    									"0", "error", "PID not found","", null, null);
	    	}
	    	tct.setMonitorAttemps(0);
	    	String status = tct.getLive();
	    	logger.debug("Status :: " + status);
	    	
	    	Date date = tct.getDate();
	    	long inter = tct.getElapsedTime();
	    	
	    	String age = TimeFormat.calcHMS(inter,TASKCONTROLLER_TIME_FORMAT);
	    	String dateFormatted = formatter.format(date);
	    	
	    	Action act = tct.getFinalAction();
	    	FileInfo[] fileOutputs = null;
	    	if (act != null)
	    	   	fileOutputs = (FileInfo[])tct.getFinalAction().getRequest().get("filesOutput");
	    	
	    	if (status != "running") {
	    		return TaskControllerCreateMessages.getOMElementResponse("monitor", jcl, step, id, dateFormatted, age, status, tct.getMessageDescription(), tct.getLogResult(), fileOutputs, tct.getResult());
	    	} 
	    	
	    	runningReturn =  TaskControllerCreateMessages.getOMElementResponse("monitor", jcl, step, id, dateFormatted, age, status, tct.getMessageDescription(), tct.getLogResult(), fileOutputs, null);
	    	sleep(sleepTime);
			currentTimeMs = System.currentTimeMillis();
			t = currentTimeMs - initTimeMs;
		}
    	return runningReturn;
    }
	
	public void sleep(int time) {
		try {
			Thread.sleep(time);
		} catch (Exception e) {
			logger.error("Can not do sleep.", e);
		}
	}
	
	public String[] getServicesID() {
		return SrvMng.getDefault().getServicesID();
	}
	
	public String[] getDescriptionServices() {
		return SrvMng.getDefault().getDescriptionServices();
	}
	
	public Collection<Service> getServicesInfo() {
		Collection<Service> services = SrvMng.getDefault().getServices();
//		callAddServices();
		
		return services;
	}
	
//	private void callAddServices(){
//		new Thread(){
//			public void run(){
//				long elapsedTime = 1000*15; // 15 seconds 
//				int iterations = 8; // 2 min in total
//				boolean error = true;	
////				TaskControllerEventEngine eventEngine = TaskControllerEventEngine.getInstance();
//				do{
//					try{
////						eventEngine.callAddServicesNode(Node.getMyself().getLocation(),Node.getMyself().getServices());
////						eventEngine.callAddServices(getServicesID(), getDescriptionServices());
//						TaskControllerToBrokerClient.callAddServices(getServicesID(), getDescriptionServices());
//						error = false;
//					} catch (Throwable t){												
//						if(--iterations == 0)							
//								logger.error("Error insert services and nodes services. ",t);
//						else {
//							try {
//								sleep(elapsedTime);
//								logger.warn("Fail insert services and nodes services. Retrying in "+elapsedTime+" miliseconds.");
//							} catch (InterruptedException e) {							
//								logger.error("Error during sleep when insert services error ocurr.");
//							}
//						}
//					}
//				} while (error && iterations > 0);
//			};
//		}.start();
//	}
	
	public OMElement getResult(String pid) throws TaskControllerException {
		if (UtilString.isNullOrEmpty(pid))
			throw new TaskControllerException("Pid not found in message.", ErrorType.ERROR);
		
		TaskControllerThread th = TaskControllerWareHouse.getInstance().getTaskControllerThread(pid);
		if (th == null)
			throw new TaskControllerException("No thread for pid " + pid, ErrorType.ERROR);
		
		OMElement omResult = th.getResult();
		if (omResult == null)
			throw new TaskControllerException("No result found for pid " + pid, ErrorType.ERROR);
		
		return omResult;
	}
	
	public Collection<String> getActivePids() {
		return ProcessConnectorMng.getInstance().getActivePids();
	}
	
	public void closeSession(String sessionId) throws AxisFault, InterruptedException, KernelException {
		logger.info("Received closed sessionId :: " + sessionId);
    	if (sessionMng.containsSession(sessionId)) {
    		List<String> pids = sessionMng.getSessionPids(sessionId);
    		logger.info("Pids for " + sessionId + " :: " + pids);
    		
    		TaskControllerThread th;
    		TaskControllerWareHouse tcWareHouse = TaskControllerWareHouse.getInstance();
    		ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
    		for (final String pid : pids) {
    			th = tcWareHouse.getTaskControllerThread(pid);
    			if (th != null) 
    				th.kill();
    			
    			/* Delete PID from TaskControllerWareHouse after 30s */
    			new Thread() {
    				public void run() {
    					setName("TC_deletePidTCWH");
    					try {
    						Thread.sleep(finishedProcessWait);
    						TaskControllerWareHouse.getInstance().remove(pid);
    		    			SrvMng.getDefault().getFinalActions().remove(pid);
    		    			logger.debug("Final Actions size :: " + SrvMng.getDefault().getFinalActions().size());
    					} catch (Throwable t) {
    						logger.error("Error deleting final action with pid " + pid, t);
    					}
    				};
    			}.start();	
    			
    			pcMng.removeProcess(pid);
    		}	
    		sessionMng.dropSession(sessionId);
    	}
    	activeSessionId = null;
    	
		// Change my own state to IDLE_NOSESSION
    	String strEvent = "closeSession";
		TaskControllerToTopologyClient.changeState(strEvent);
		
		/* Delete cache folders */
		GridCache cache = GridCacheFactory.getInstance(CachePolicy.LESS_DOWNLOAD_EFFORT);
		if (!cache.deleteCachedFolders())
			logger.warn("Delete cache folder on close session is disabled, check configuration");
	}
	
	public OMElement getXMLDocument(String xmlName) throws TaskControllerException {
		OMElement documentElement=null;
		String file = "";
    	try {
	    	if (xmlName.equalsIgnoreCase("taskControllerFlow"))	{
	    		file = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_SERVICE_FILE);
	    		StAXOMBuilder builder = new StAXOMBuilder(new FileInputStream(file));
	    		documentElement = builder.getDocumentElement();
	    	}
    	} catch (XMLStreamException xmlex) {
			throw new TaskControllerException("Error reading file xml :: " + file, ErrorType.ERROR);
    	} catch (FileNotFoundException fnfex) {
    		throw new TaskControllerException("File " + file + " not found.", ErrorType.ERROR);
    	}
    	return documentElement;
	}
	
	/**Starts a new Process Connector. This method is intended to be called only from the monitor,
	 * when the status of the node is "black" -no process connector running on its IP.
	 * @param nPort
	 * @throws TaskControllerException
	 */
	public void startProcessConnectorFromMonitor(int nPort) throws IOException, InterruptedException, ProcessConnectorException {
		if (m_theThread.getState() == Thread.State.TERMINATED) {
			// Tries to start up a new Process Connector just once (in one port - no more trials).
//			TaskControllerLogicalThread.setS_nPCPort(nPort);
//			TaskControllerLogicalThread.setS_nPCMaxPort(nPort);
//			GridConfiguration.getDefault().setParamString( ConfigurationParam.PC_BINDING_PORT, Integer.toString( nPort));
			
//			boolean bIStartedPCup= m_theThread.tryProcessConnector( nPort); // This is not necessary if self PC starting up is enabled again
//			m_theThread= new TaskControllerLogicalThread();
			m_theThread= new TaskControllerLogicalThread(nPort);
			m_theThread.start();
			
//			if ( bIStartedPCup)
				TaskControllerToTopologyClient.checkAndStartUpDBIfNecessary();
		} else
			throw new ProcessConnectorException( "Process Connector running or attemping to start up", ErrorType.CRITICAL);
	}
	
	public void checkProcessConnector() throws InterruptedException, ProcessConnectorException {
		if ( m_theThread.getState()==Thread.State.TERMINATED) {
			// Starts up a new checking thread, just in case the process connector is really working.
			m_theThread= new TaskControllerLogicalThread();
			m_theThread.start();			
		} else
			throw new ProcessConnectorException( "Process Connector running or attemping to start up", ErrorType.WARNING);
	}

	/**Receives a gz DB copy and places it into its corresponding folder -specified in the .xml configuration files.
	 * @param nameBBDD name of the database. Used to look for the corresponding folder in the configuration params.
	 * @param dhDBZip Contains the gz copy of the DB.
	 * @throws Exception 
	 */
	public void replicateDB(String nameBBDD, DataHandler dhDBZip) throws Exception {	
		final String strDBDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.DUMP_MEMORY_DIR);	
		try {
			// Convert data handler into gz File
			
			File fileDBGz = new File( strDBDir + File.separator + nameBBDD + ".gz");

			FileOutputStream outputStream = new FileOutputStream( fileDBGz);
			dhDBZip.writeTo( outputStream);
		} catch (IOException e) {
			logger.warn("Unable gz database file",e);
		}
	}	
	
	public int synchronizeUpdate() throws ProcessConnectorException, InterruptedException {
		String pcWorkingDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_WORKING_DIR);
		ProcessConnectorBinding prConBind = new ProcessConnectorBinding( pcWorkingDir,
				"", "", System.currentTimeMillis()+":syncUpdate");
		prConBind.syncUpdateProcess(); 
		ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(prConBind);
		//0 si hay update y bajado
		//1 no hay update
		//eoc error
		logger.debug("Exit Code synchronizing update :: " + output.getExitCode());	
		return output.getExitCode();
	}
	
	public String getProcessStatistics(String pid) throws ProcessConnectorException, InterruptedException {
		String pcWorkingDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_WORKING_DIR);
		ProcessConnectorBinding connector = new ProcessConnectorBinding(pcWorkingDir, "getProcessStatistics", 
				pid, System.currentTimeMillis() + ":statistics");
		ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
		pcMng.startProcess(connector);
		ShellCommandOutput output = pcMng.monitor(connector);		
		return output.getFullOuput();	
	}
	
	public String asyncronousProcess(String[] params, String cmd, String pid) throws Throwable {
		logger.debug("CMD :: " + cmd);
		
		Vector<Object> processParams = new Vector<Object>(params.length+1);	
		for (int i = 0, iUntil=params.length; i < iUntil; i++) {
			logger.debug("PARAM::" + params[i]);
			if (params[i].contains(":"))
				processParams.add(params[i].substring(params[i].indexOf(":")+1,params[i].length()));
		}
		if (cmd.contains("GRID_CANCEL_PROCESS") && (processParams.size() > 0)) {
			String killResponse = asyncronousKill((String)processParams.get(0));
			if (!killResponse.equals("KILLED_OK"))
				return killResponse;
		}
		return ProcessConnectorMng.getInstance().asyncronousProcess(pid, cmd, processParams);
	}
	
	public String asyncronousKill(String pid) {
		String response = "";
		try {
			TaskControllerThread th = null;
			if (!UtilString.isNullOrEmpty(pid))
				th = TaskControllerWareHouse.getInstance().getTaskControllerThread(pid);
			
			if (th == null)
				response = "Error.No process for pid " + pid;
			else {
				th.kill();
				response = "KILLED_OK";
				executingProcess = false;
			}
		} catch (Throwable th) {
			logger.error("Error killing process taskcontroller ", th);
			response = "Error killing process taskcontroller ";
		} finally {
			logger.info("END - Response --> " + response);
		}
		return response;
	}	
	
	/*
	public void restart(){
		ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
		pcMng.restart();
	}
	
	public void restartRoot(){
		ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
		pcMng.restartRoot();
	}
	*/	
	
	public String setXMLDocument(String documentTitle, String contentElement) throws TaskControllerException {
		String response="ok";
    	try {
    		String headerString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    		if (documentTitle.equalsIgnoreCase("taskControllerFlow")) {
    			String file = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_SERVICE_FILE)+".template";
    			String newXml = "";
    			if (!contentElement.contains("<?xml version="))
    				newXml = headerString + contentElement;
    			else 
    				newXml = contentElement;
    		    
//    			if (contentElement == null) 
//    				throw new TaskControllerException("Error. Invalid XML format.", ErrorType.ERROR);
//    			
//    			else 
//    			{
    				ParserXML.textToXML(newXml);	    		
	    			BufferedWriter out = new BufferedWriter(new FileWriter(file));
	    			out.write(newXml);
	    			out.close();
	    			restartServices();
//    			}
    		}
    	} catch (IOException ioex) {
    		throw new TaskControllerException("Error writing XML document.", ErrorType.ERROR);
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		throw new TaskControllerException("Error restarting services.", ErrorType.ERROR);
    	}
		return response;
	}
	
	public void restartServices() throws TaskControllerException, Exception {
    	String file = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_SERVICE_FILE);
		logger.info("Restarting services from " + file);
		SrvMng.getDefault().load(file);	
		TaskControllerToBrokerClient.notifyChangeXmlToRoot();
	}
	
	public void changeTopologyConfig(String newRootLocation, String gridName) {
		GridConfiguration conf = GridConfiguration.getDefault();  	
		String actualRoot = conf.getParameter(ConfigurationParam.ROOT_LOCATION);
		String ip_ActualRoot = actualRoot.substring(0,actualRoot.indexOf(":")); 		
		String ip_newRoot = newRootLocation.substring(0,newRootLocation.indexOf(":"));
		
		//Change BBDD URL    		    		
		String bbddURL = conf.getParameter(ConfigurationParam.MDS_BBDD_URL);
		logger.info("Old bbdd URL::" + conf.getParameter(ConfigurationParam.MDS_BBDD_URL));
		String newBBDDURL = bbddURL.replace(ip_ActualRoot, ip_newRoot);
		conf.setParamString(ConfigurationParam.MDS_BBDD_URL, newBBDDURL);
		logger.info("New bbdd URL::" + conf.getParameter(ConfigurationParam.MDS_BBDD_URL));
		
		conf.setParamString(ConfigurationParam.ROOT_LOCATION, newRootLocation);
		if (!gridName.equalsIgnoreCase("")) {
			conf.setParamString(ConfigurationParam.ROOT_GRID_NAME, gridName);
			conf.setParamString(ConfigurationParam.MDS_BBDD_NAME, gridName);
		}
	}
	
	public Map<String, String> getPartialLogs(String pid) {
		/* Map<filename, contentFile> partialLogs */
		Map<String, String> partialLogs = new HashMap<String, String>();
	
		/* Get filenames corresponding to the PID */
		List<String> filenames = getFileNames(pid.substring(pid.lastIndexOf(":")+1));
		
		String contentFile="";
		for (int i = 0, iUntil=filenames.size(); i < iUntil; i++) {
			contentFile = FileOperations.readFile(filenames.get(i));
			partialLogs.put(filenames.get(i), contentFile);
		}
		return partialLogs;
	}
	
    public List<String> getFileNames(String pid) {
		List<String> filenames = new ArrayList<String>();
		String directoryName = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_LOG_DIR);
		String processLogDirPrefix = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_PROCESSESLOG_PREFIX);
		File directory = new File(directoryName);
		if (!directory.exists())
			logger.error("Directory not exists :: " + directoryName);
		else {
			File[] processLogFiles, directoryFiles = directory.listFiles();
			File pcLogDirFile;
			String filename ;
			for (int i=0, iUntil=directoryFiles.length; i < iUntil; i++) {
				pcLogDirFile = directoryFiles[i];
				if ((pcLogDirFile.isDirectory()) && (pcLogDirFile.getName().startsWith(processLogDirPrefix))) {
					/* Find all files containing the PID in the filename */
					processLogFiles = pcLogDirFile.listFiles(); 
					for (int j=0, jUntil=processLogFiles.length; j < jUntil; j++) {
						filename = processLogFiles[j].getAbsolutePath();
						if (filename.contains(pid))
							filenames.add(filename);
					}
				}
			}
		}
		return filenames;
	}
    
    public String getPartialLogFile(String file) {
    	return FileOperations.readFile(file);
    }
	
	private void zipFiles(List<File> listFiles, String strZipPath) throws IOException {
		byte[] buf = new byte[1024];
	    
    	File file;
    	FileInputStream in;
    	FileOutputStream os= new FileOutputStream(strZipPath);
		ZipOutputStream out = new ZipOutputStream(os);		
		
        for (int i=0, iUntil=listFiles.size(); i<iUntil; i++) {
        	file= listFiles.get(i);
        	out.putNextEntry(new ZipEntry(file.getName()));
        	if ((file.length()>0)) {
        		in = new FileInputStream(file);
	        	int len;
	        	while((len = in.read(buf)) > 0) {
	        		out.write(buf, 0, len);
	        	}
	        	in.close();
        	}
        	out.closeEntry();
        }
        out.flush();
        os.flush();
        out.close();
        os.close();
	}

	@SuppressWarnings("unchecked")
	public DataHandler requestAlerts(String strTimeSince) throws IOException {
		String alertDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_ALERTS);
		File alertDirectory = new File(alertDir);
//		File[] allAlertFiles = new File(alertDir).listFiles();
		ArrayList<File> allAlertFiles = new ArrayList<File>();
		allAlertFiles.addAll(FileUtils.listFiles(alertDirectory, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE));
		long timeSince = Long.parseLong(strTimeSince);

		ArrayList<File> alertsToSend= new ArrayList<File>();
		
		File alertFile;
		String strFile="", strAlertTime="";
		long alertTime;
		for(int i=0, iUntil=allAlertFiles.size(); i<iUntil; i++) {
			alertFile= allAlertFiles.get(i);
			strFile= alertFile.getName();
			if (strFile.startsWith("errormsg") || strFile.startsWith("infomsg") || strFile.startsWith("warnmsg")) {				
				strAlertTime = alertFile.getName().substring( strFile.indexOf("_")+1, strFile.indexOf(".xml"));
				alertTime= Long.parseLong(strAlertTime);
				if (alertTime>=timeSince)
					alertsToSend.add(alertFile);	
			}
		}
		if (alertsToSend.size()>0) {	
			logger.info("Zipping alertFiles");
			String zipAbsolutePath = alertDir + File.separator + "alerts.zip";
			zipFiles(alertsToSend, zipAbsolutePath);
			FileDataSource fileDataSource = new FileDataSource(zipAbsolutePath);
			return new DataHandler(fileDataSource);
		} else
			return null;
				
//		// Delete .zip file
//		if ( ! ( new File( zipAbsolutePath)).delete())
//			logger.warn( "Unable to delete " + strDBDir + File.separator + strDBName + ".zip");
	}
	
	public boolean deleteAlert(String strFile)	{
		boolean deleted = new File(strFile).delete();
		String dirPath = strFile.substring(0, strFile.lastIndexOf("/"));
		File dir = new File(dirPath);
		if (dir.listFiles().length == 0) {
			dir.delete();
		}
		return deleted;
	}
	
	public DataHandler requestProcessLogs(List<String> strFilePid, List<String> listDate, 
			List<String> listType) throws IOException {
		DataHandler dh = null;
		String pcLogDirName = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_LOG_DIR);
		String processLogDirPrefix = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_PROCESSESLOG_PREFIX);
		String mdsFile = MdsProvider.getOutcomefolder();
		String homeDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_APP_DIR);
		String pid, loggingFile, logFile;
		List<String> filteredAndFormattedDates = null;
		List<String> filteredAndFormattedDatesForNode = null;
		Properties p;
		File pcLogDir = new File(pcLogDirName);
		File pcLogDirFile, processLogFile, logMainDir;
		File[] pcLogDirFiles, processLogFiles;
		ArrayList<File> logsToSend= new ArrayList<File>();
		int l = 0, iSize = 0;
		
		boolean processLogsRequested = listType.contains("processLogs");
		boolean nodeLogsRequested = listType.contains("nodeLogs");
		boolean pcLogsRequested = listType.contains("pcLogs");
		boolean controlProcLogsRequested = listType.contains("controlProcLogs");
		
		if (processLogsRequested) {
			l = 0;
			iSize = strFilePid.size();
			while(l < iSize){
				pid = strFilePid.get(l).substring(strFilePid.get(l).lastIndexOf(":")+1);
				strFilePid.set(l,pid);
				l++;
			}
			pcLogDirFiles = pcLogDir.listFiles();
			for (int i=0, iUntil=pcLogDirFiles.length; i<iUntil; i++) {
				pcLogDirFile = pcLogDirFiles[i];
				if ((pcLogDirFile.isDirectory()) && (pcLogDirFile.getName().startsWith(processLogDirPrefix))) {
					/* Find all files containing the PID in the filename */
					processLogFiles = pcLogDirFile.listFiles(); 
					for (int j=0, jUntil=processLogFiles.length; j<jUntil; j++) {
						processLogFile = processLogFiles[j];
						l=0;
						iSize=strFilePid.size();
						while(l<iSize){
							if (processLogFile.getName().toLowerCase().contains(strFilePid.get(l).toLowerCase()))
								logsToSend.add(processLogFile);
							l++;
						}
					}
				}
			}
		}
		if (pcLogsRequested || controlProcLogsRequested || nodeLogsRequested) {
			filteredAndFormattedDates = filterAndFormatDatesRequested(listDate);
			filteredAndFormattedDatesForNode = filterAndFormatDatesRequestedForNode(listDate);
			loggingFile = GridConfiguration.getDefault().getParameter(ConfigurationParam.LOGGING_FILE_CONF);
			p = new Properties();
			p.load(new FileInputStream(loggingFile));
			logFile = (String) p.get("log4j.appender.DAILY.File");
			logMainDir = new File(logFile.substring(0,logFile.lastIndexOf("/")));
			if (!logMainDir.exists())
				logger.error("Directory " + logMainDir.getAbsolutePath() + " not exists.");
			pcLogDirFiles = logMainDir.listFiles();
			mdsFile = MdsProvider.getOutcomefolder();
			homeDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_APP_DIR);
			Date dToday = Calendar.getInstance().getTime();
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String formattedToday = dateFormat.format(dToday);
			
			for (int i=0, iUntil=pcLogDirFiles.length; i<iUntil; i++) {
				pcLogDirFile = pcLogDirFiles[i];
				if (nodeLogsRequested) {
					if (checkNodeLogByDateForProcessesDownload(pcLogDirFile.getName(), filteredAndFormattedDatesForNode, formattedToday))
						logsToSend.add(pcLogDirFile);
				}
				if (pcLogsRequested) {
					if (!pcLogDirFile.isDirectory() && checkPcLogByDate(pcLogDirFile.getName(), filteredAndFormattedDates))
						logsToSend.add(pcLogDirFile);
				}
				if (controlProcLogsRequested) {
					if (!pcLogDirFile.isDirectory() && checkControlProcLogByDate(pcLogDirFile.getName(), filteredAndFormattedDates))
						logsToSend.add(pcLogDirFile);
				}
			}
		}
		if ( logsToSend.size()>0) {	
			String zipAbsolutePath = homeDir + File.separator + mdsFile + File.separator + "process_logs_" + Node.getMyself().getLocation().replace(":","_") + ".zip";
			logger.info( "Zipping logsFiles to " +zipAbsolutePath);
			zipFiles( logsToSend, zipAbsolutePath);
			FileDataSource fileDataSource = new FileDataSource( zipAbsolutePath);
			dh = new DataHandler(fileDataSource);
		}
		return dh;
	}
	
	private List<String> filterAndFormatDatesRequested(List<String> listDate) {
		ArrayList<String> result = new ArrayList<String>();
		
		Iterator<String> itList = listDate.iterator();
		String frmDate, date;
		while (itList.hasNext()) {
			date = itList.next();
			frmDate = date.substring(0, date.indexOf(' ')).replace("-", "");
			if (!result.contains(frmDate))
				result.add(frmDate);
		}
		return result;
	}
	
	private List<String> filterAndFormatDatesRequestedForNode(List<String> listDate) {
		ArrayList<String> result = new ArrayList<String>();
		
		Iterator<String> itList = listDate.iterator();
		String frmDate, date;
		while (itList.hasNext()) {
			date = itList.next();
			frmDate = date.substring(0, date.indexOf(' '));
			if (!result.contains(frmDate))
				result.add(frmDate);
		}
		return result;
	}

	public DataHandler requestNodesLogs(List <String> dates) throws IOException {           
		String loggingFile = GridConfiguration.getDefault().getParameter(ConfigurationParam.LOGGING_FILE_CONF);
		Properties p = new Properties();   
		p.load(new FileInputStream(loggingFile));
		String logFile = (String) p.get("log4j.appender.DAILY.File");
		File logMainDir = new File(logFile.substring(0,logFile.lastIndexOf("/")));
		if (!logMainDir.exists())
			logger.error("Directory " + logMainDir.getAbsolutePath() + " not exists.");
          
		String mdsFile = MdsProvider.getOutcomefolder();
		String homeDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_APP_DIR);
    
		ArrayList<File> logsToSend= new ArrayList<File>();
          
		File[] pcLogDirFiles = logMainDir.listFiles();
		File pcLogDirFile;
		for (int i=0, iUntil=pcLogDirFiles.length; i<iUntil; i++) {
			pcLogDirFile = pcLogDirFiles[i];
			if (checkNodeLogByDate(pcLogDirFile.getName(), dates))
				logsToSend.add(pcLogDirFile);  
		}
          
		if (logsToSend.size()>0) {     
			String zipAbsolutePath = homeDir + File.separator + mdsFile + File.separator + "nodes_logs.zip";
			zipFiles(logsToSend, zipAbsolutePath);
			FileDataSource fileDataSource = new FileDataSource(zipAbsolutePath);
			return new DataHandler(fileDataSource);
		} else
			return null;
	}
	
	public DataHandler requestPcLogs(List <String> dates) throws IOException {           
		String loggingFile = GridConfiguration.getDefault().getParameter(ConfigurationParam.LOGGING_FILE_CONF);
		Properties p = new Properties();   
		p.load(new FileInputStream(loggingFile));
		String logFile = (String) p.get("log4j.appender.DAILY.File");
		File logMainDir = new File(logFile.substring(0,logFile.lastIndexOf("/")));
		if (!logMainDir.exists())
			logger.error("Directory " + logMainDir.getAbsolutePath() + " not exists.");
          
		String mdsFile = MdsProvider.getOutcomefolder();
		String homeDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_APP_DIR);
		
		ArrayList<File> logsToSend= new ArrayList<File>();
          
		File[] pcLogDirFiles = logMainDir.listFiles();
		File pcLogDirFile;
          
		for (int i=0, iUntil=pcLogDirFiles.length; i<iUntil; i++) {
			pcLogDirFile = pcLogDirFiles[i];
			if (!pcLogDirFile.isDirectory() && checkPcLogByDate(pcLogDirFile.getName(), dates))
				logsToSend.add(pcLogDirFile);
		}
          
		if (logsToSend.size()>0) {
			String zipAbsolutePath = homeDir + File.separator + mdsFile + File.separator + "nodes_logs.zip";
			zipFiles( logsToSend, zipAbsolutePath);
			FileDataSource fileDataSource = new FileDataSource( zipAbsolutePath);
			return new DataHandler(fileDataSource);
		} else
			return null;
    }
	
	public DataHandler requestControlProcLogs(List <String> dates) throws IOException {
		String loggingFile = GridConfiguration.getDefault().getParameter(ConfigurationParam.LOGGING_FILE_CONF);
		Properties p = new Properties();   
		p.load(new FileInputStream(loggingFile));
		String logFile = (String) p.get("log4j.appender.DAILY.File");
		File logMainDir = new File(logFile.substring(0,logFile.lastIndexOf("/")));
		if (!logMainDir.exists())
			logger.error("Directory " + logMainDir.getAbsolutePath() + " not exists.");
          
		String mdsFile = MdsProvider.getOutcomefolder();
		String homeDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.SC_APP_DIR);
		
		ArrayList<File> logsToSend= new ArrayList<File>();         
		
		File[] pcLogDirFiles = logMainDir.listFiles();
		File pcLogDirFile;
		
		for (int i=0, iUntil=pcLogDirFiles.length; i<iUntil; i++) {
			pcLogDirFile = pcLogDirFiles[i];
			if (!pcLogDirFile.isDirectory() && checkControlProcLogByDate(pcLogDirFile.getName(), dates))
				logsToSend.add(pcLogDirFile);
		}
          
		if (logsToSend.size()>0) {     
			String zipAbsolutePath = homeDir + File.separator + mdsFile + File.separator + "nodes_logs.zip";
			zipFiles( logsToSend, zipAbsolutePath);
			FileDataSource fileDataSource = new FileDataSource( zipAbsolutePath);
			return new DataHandler(fileDataSource);
		} else
			return null;
    }
    
	private boolean checkNodeLogByDateForProcessesDownload(String filename, List<String> dates, String formattedToday) {
		boolean check = false;
		for (String date : dates) {
			if(date.equalsIgnoreCase(formattedToday) && filename.contains(Node.getMyself().getPort()) && filename.contains(Node.getMyself().getName().substring(0, Node.getMyself().getName().lastIndexOf("."))+"-") && filename.endsWith(".log") && filename.startsWith("img"))
				check = true;
			else if(filename.contains(Node.getMyself().getPort()) && filename.contains(Node.getMyself().getName().substring(0, Node.getMyself().getName().lastIndexOf("."))+"-") && filename.contains(".log") && filename.contains(date) && filename.startsWith("img"))
				check = true;
		}
		return check;
	}
	
    private boolean checkNodeLogByDate(String filename, List<String> dates) {
    	boolean check = false;
    	for (String date : dates) {
    		if(date.equalsIgnoreCase("today") && filename.contains(Node.getMyself().getPort()) && filename.contains(Node.getMyself().getName().substring(0, Node.getMyself().getName().lastIndexOf("."))+"-") && filename.endsWith(".log") && filename.startsWith("img"))
    			check = true;
    		else if(filename.contains(Node.getMyself().getPort()) && filename.contains(Node.getMyself().getName().substring(0, Node.getMyself().getName().lastIndexOf("."))+"-") && filename.contains(".log") && filename.contains(date) && filename.startsWith("img"))
    			check = true;
    	}
    	return check;
    }
    
    private boolean checkPcLogByDate(String filename, List<String> dates) {
    	boolean check = false;
    	String formattedDate = "";
    	for (String date : dates) {
    		if (date.equalsIgnoreCase("today")) {
    			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    			Date d = new Date();
    			formattedDate = dateFormat.format(d).replace("-", "");
    		}
    		else
    			formattedDate = date.replace("-", "");
    		if (filename.endsWith(formattedDate+".txt") && filename.startsWith("XMLRPC_SERVER.log"))
    			check = true;
    	}
    	return check;
    }
    
    private boolean checkControlProcLogByDate(String filename, List<String> dates) {
    	boolean check = false;
    	String formattedDate = "";
    	for (String date : dates) {
    		if (date.equalsIgnoreCase("today")) {
    			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    			Date d = new Date();
    			formattedDate = dateFormat.format(d).replace("-", "");
    		}
    		else
    			formattedDate = date.replace("-", "");
    		if (filename.endsWith(formattedDate+".txt") && filename.startsWith("CONTROL_PROCESS.log"))
    			check = true;
    	}
    	return check;
    }
	
	public void launchPDMMonitor() throws ProcessConnectorException, InterruptedException {	
		/* CMD: Process connector program that executes monitoring */
		String cmdPDMMonitor = "rexx PDM_MONITOR";
		/* Params: Time interval (seconds) in the requests will be made to the PDM */
		GridConfiguration conf = GridConfiguration.getDefault();
		List<?> listNamesMonitorParams = conf.getList(ConfigurationParam.MONITOR_PARAMS_NAME);
		List<?> listValuesMonitorParams = conf.getList(ConfigurationParam.MONITOR_PARAMS_VALUE);
		
		String timeInterval = "10";
		if (!listNamesMonitorParams.contains("PDMTimeInterval"))
			logger.warn("Parameter time interval not found. Default is 10 seconds.");
		else {
			int paramIndex = listNamesMonitorParams.indexOf("PDMTimeInterval");			
			timeInterval = (String)listValuesMonitorParams.get(paramIndex);
		}
		
		logger.info("Interval time for PDM Monitor :: " + timeInterval);
		Vector<Object> pdmMonitorParams = new Vector<Object>(1);
		pdmMonitorParams.add(timeInterval);
		String pidPDMMonitor = System.currentTimeMillis() + ":monitorPDM";
		
		ProcessConnectorMng.getInstance().startProcessNoMonitoring(pidPDMMonitor, cmdPDMMonitor, pdmMonitorParams);
	}
	
	public boolean deleteCacheFolder(HashMap<String,ArrayList<String>> map) {	
		boolean allOK = true;
		
		File folder, file;
		File[] files;
		ArrayList<String> arrExt = new ArrayList<String>();
		Iterator<Entry<String,ArrayList<String>>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			arrExt = null;
			Entry<String,ArrayList<String>> e = it.next();
			
			folder = new File((String) e.getKey());
			arrExt = (ArrayList<String>) e.getValue();
			if (folder.exists()) {
				files = folder.listFiles();
				for(int j=0, jUntil=files.length; j<jUntil; j++) {
					file = files[j];
					if(file.isFile() && hasCorrectExtension(file.getAbsolutePath(),arrExt)) {
						if (!file.delete()) 
							allOK = false;
						else
							logger.info("#### File: "+file.getAbsolutePath()+" DELETED");
					}
				}
			}
		}
		return allOK;
	}

	private boolean hasCorrectExtension(String filename, ArrayList<String> arrExt) {
		boolean found = false;
		int i = 0;
		int iUntil = arrExt.size();
		while (!found && (i<iUntil)) {
			if (filename.endsWith("." + arrExt.get(i)))
				found = true;
			i++;
		}
		return found;
	}
	
	/**Receives a zipped file and places it into its corresponding directory.
	 * @param dhZipFile Contains the zipped file.
	 * @throws TaskControllerException if the blocking file could no be delete. This error can be critical due to new replications won't be done.
	 */
	public void replicateZipFile(DataHandler dhZipFile) throws TaskControllerException {
		/* Get the directory where you have to unzip the file with the dumped data */
		String dumpDir= GridConfiguration.getDefault().getParameter(ConfigurationParam.DUMP_MEMORY_DIR);
		if (dumpDir == null) 
			throw new TaskControllerException("Error getting dump memory directory from broker.xml", ErrorType.ERROR);

		if (!dumpDir.endsWith(File.separator))
			dumpDir = dumpDir + File.separator;
		
		/* Create directory if it does not exist */
		File dumpDirFile = new File(dumpDir);
		if (!dumpDirFile.exists())
			dumpDirFile.mkdir();
		
		/* Temporary name of the zip file*/
		String tmpNameZipFile = String.valueOf(System.currentTimeMillis());
		
		File zipFile = new File(dumpDir + tmpNameZipFile + ".zip");
		
		try {	
			// Convert data handler into zip File	
			FileOutputStream outputStream = new FileOutputStream(zipFile);
			dhZipFile.writeTo(outputStream);
			
			// Unzip file into the right directory.
			unzip(dumpDir, tmpNameZipFile);
		} catch (IOException ioex) {
			logger.error("Error unzipped file " + tmpNameZipFile + " in directory " + dumpDir);
			throw new TaskControllerException("Error unzipped file.", ioex);
		} finally {
			try {
				zipFile.delete();
			} catch (Exception ex) {
				logger.warn("Temporary file could not be deleted", ex);
			}
		}
	}
	
	public String requestDeletedAlerts(String type, List <String> listFiles) {	
		String bOk = "OK";
		String alertsPathName = GridConfiguration.getDefault().getParameter(ConfigurationParam.MDS_ALERTS);
		File alerts = new File(alertsPathName);
		File [] alertsList = alerts.listFiles();
		
		File alertFile;
		for (int i=0, iUntil=alertsList.length; i<iUntil; i++) {
			alertFile = alertsList[i];
			if(type.equals("all")&& alertFile.getName().endsWith(".xml")) {	
				if(!alertFile.delete())
					bOk = "error";
			} else if(type.equals("filtered")) {
				for (int j=0, jUntil=listFiles.size(); j<jUntil;j++)	{
					if(alertFile.getName().equals(listFiles.get(j))) {
						listFiles.remove(j);
						if(!alertFile.delete())
							bOk = "error";
					}
				}
			}
		}
		logger.info("bOk::"+bOk);
		return bOk;
	}
	
	private void unzip(String strDir, String strName) throws IOException {
		String strZip= strDir + File.separator + strName + ".zip";
		ZipFile zipFile = new ZipFile(strZip);
		Enumeration<?> entries= zipFile.entries();
		
		while (entries.hasMoreElements()) {
			ZipEntry entry = (ZipEntry)entries.nextElement();
			
			if(entry.isDirectory())	{
				(new File(strDir + File.separator + entry.getName())).mkdir();
			} else {
				FileOperations.copyFile( zipFile.getInputStream( entry), new BufferedOutputStream( 
						new FileOutputStream( strDir+ File.separator + entry.getName())));
			}
		}
		zipFile.close();
		// Delete .zip
		if (!(new File( strZip)).delete())
			logger.warn( "Unable to delete " + strZip);
	}
	
	public void stopNode() {
		System.exit(0);
	}
	
	public String getXMLFileName(String xmlName) {
		String file = "";
	    if (xmlName.equalsIgnoreCase("taskControllerFlow")) {
	    	file = GridConfiguration.getDefault().getParameter(ConfigurationParam.TC_SERVICE_FILE);
	    }
    	return file;
	}
	
	public void killProcessConnector() throws Exception {
		ProcessConnectorManager.getInstance().setStarted(false);
//		if (!ProcessConnectorMng.isNullInstance())
			ProcessConnectorMng.getInstance().killProcessConnector();
	}
	
	/**Receives a PC parms file copy and places it into its corresponding folder.
	 * @param dhPCZip Contains the copy of the PC parms file.
	 * @throws Exception 
	 */
	public void replicatePC(DataHandler dhPCZip) throws Exception {	
		final String strPCDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_WORKING_DIR);	
		try {
			// Convert data handler into gz File
			
			File filePC = new File( strPCDir + File.separator + "XMLRPC_SERVER_PARM_L.txt");

			FileOutputStream outputStream = new FileOutputStream( filePC);
			dhPCZip.writeTo( outputStream);
			changeLocationsPcParms(filePC);
		} catch (IOException e) {
			logger.warn("Unable create pc parms file",e);
		}
	}

	private void changeLocationsPcParms(File filePC) {
		File targetFileDir =  new File((String)GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_REXX_DIR) + File.separator);
		File tmpXmlrpcServerParamFile =  new File(targetFileDir.getAbsolutePath() + File.separator + "XMLRPC_SERVER_PARM_L.txt");
		PrintWriter pw = null;
		BufferedReader in = null;
		String location = Node.getMyself().getIp();
		
		String wkDir = GridConfiguration.getDefault().getParameter(ConfigurationParam.PC_LOG_DIR);
		wkDir = wkDir.substring(0, wkDir.indexOf("/logs"));
		logger.info("Change File XMLRPC_SERVER_PARM_L.txt with my ip_addr= " + location+" and my working path= "+wkDir);
		try {
			in = new BufferedReader(new FileReader(filePC));
			String str;
			String oldLocation = "", oldTmpDir = "", filecontent = "";
			while ((str = in.readLine()) != null) {
				if (str.contains("_ip_addr")) {
					oldLocation = str.substring(str.indexOf("'")+1, str.lastIndexOf("'"));
					str = str.replace(oldLocation, location);
				}
				if (str.contains("_DIR_TMP_") && str.contains("='")) {
					oldTmpDir = str.substring(str.indexOf("'")+1, str.lastIndexOf("'"));
					str = str.replace(oldTmpDir, wkDir);
				}
				filecontent += str + System.getProperty("line.separator");
	        }
			in.close();
			FileWriter inputFile = new FileWriter(tmpXmlrpcServerParamFile);
			pw = new PrintWriter(inputFile);
			pw.write(filecontent);
		} catch (IOException ioex) {
	    	logger.error("Unable to read file xmlrpc server parm contents", ioex);
		} finally {
			try {
				if (in != null) in.close();
				if (pw != null) pw.close();
			} catch (IOException ioex) {
				ioex.printStackTrace(System.out);
				logger.error("Unable to close file xmlrpc server parm", ioex);
			}
		}
	}

	public void setExecutingProcess(boolean executingProcess) {
		this.executingProcess = executingProcess;
	}
}