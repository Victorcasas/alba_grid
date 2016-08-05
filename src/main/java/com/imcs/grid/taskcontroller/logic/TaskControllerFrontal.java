package com.imcs.grid.taskcontroller.logic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataHandler;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;

import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.error.KernelException;
import com.imcs.grid.taskcontroller.Service;
import com.imcs.grid.taskcontroller.TaskControllerException;

public class TaskControllerFrontal {
	
	private static TaskControllerFrontal instance = null;
	private TaskControllerLogical logical = null;
	
	public static TaskControllerFrontal getInstance() throws TaskControllerException
	{
		if (instance==null)
			instance = new TaskControllerFrontal();
		return instance;
	}
	
	public static TaskControllerFrontal newInstance() 
	{
		return instance;
	}
	
	private TaskControllerFrontal()
	{
		logical = TaskControllerLogical.getInstance();	
	}
	
//	private TaskControllerFrontal() 
//	{
//		SrvMng srvMng = SrvMng.getDefault();
//		GridConfiguration conf = GridConfiguration.getDefault();
//		String file = conf.getParameter(ConfigurationParam.TC_SERVICE_FILE);
//		logger.info(ConfigurationParam.TC_SERVICE_FILE + " : " + file);
//		try {
//			srvMng.load(file);
			/* It begins the management of the main log */
//			MainLogs ml = new MainLogs();
//			ml.init();
			
			/* It begins the management of the processes log (reports, cobol, sort, ...) that starts 
			   with ppg or bpg */
//			ProcessLogs wd = new ProcessLogs();
//			String[] listLogDirs = {
////						srvMng.getGlobalParams().get("io-working-dir"),
////						srvMng.getGlobalParams().get("fc-working-dir"),
//						conf.getParameter(ConfigurationParam.PC_LOG_DIR)
//			};
//			String reportDir = MdsProvider.getOutcomefolder();
//			wd.init(listLogDirs, reportDir);
			
			/* It begins the management of the services log such as log files ending in .cob or .srt */
//			ServicesLogs sl = new ServicesLogs();
//			sl.init();
			
//			PDMLogs pl = new PDMLogs();
//			pl.init();

//			PDMStatsData ps = new PDMStatsData();
//			ps.init();
			
//		} 
//		catch (TaskControllerException we) {
//			logger.error("Error reading services.", we);
//		}
//		sessionMng = TaskControllerSessionMng.getInstance();
//		m_theTCLogical= TaskControllerLogical.getInstance();		
//	}
	
	public String performTask(OMElement gridJobMessage) 
	{
		return logical.performTask(gridJobMessage);
	}
	
	public List<OMElement> getAllSubmittedMessages() 
	{
		return logical.getAllSubmittedMessages();
	}
	
	public OMElement getSubmittedMessage(String pid) throws TaskControllerException
	{
		return logical.getSubmittedMessage(pid);
	}
	
    public OMElement allocate(String service, OMElement ome) throws Throwable 
    {
    	return logical.allocate(service, ome);
    }
    
	public OMElement monitor(OMElement ome, String totalTime, String serviceTime) 
	{
		return logical.monitor(ome,totalTime,serviceTime);
	}
    	
    public String[] getServicesID() 
    {
    	return logical.getServicesID();
    }
    
    public String[] getDescriptionServices() 
    {
    	return logical.getDescriptionServices(); 
    }
    
    public Collection<Service> getServicesInfo() 
    {
    	return logical.getServicesInfo();
    }
    
    public OMElement getResult(String pid) throws TaskControllerException
    {
    	return logical.getResult(pid);
    }
    
    public Collection<String> getActivePids() 
    {
    	return logical.getActivePids();
    }
    
    public void changeXML(String operation) throws Exception 
    {
    	if (operation.equalsIgnoreCase("restartServices"))
    		logical.restartServices();
    }
    
    public void closeSession(String sessionId) throws AxisFault, InterruptedException, KernelException
    {
    	logical.closeSession(sessionId);
    }
    
    public OMElement getXMLDocument(String xmlName) throws TaskControllerException
    {
    	return logical.getXMLDocument(xmlName);
    }
    
    public String setXMLDocument(String documentTitle, String contentElement) throws TaskControllerException
    { 
    	return logical.setXMLDocument(documentTitle, contentElement);
    }
    
    public void changeTopologyConfig(String newRootLocation, String gridName) 
    {
    	logical.changeTopologyConfig(newRootLocation, gridName);
    }
    
    public Map<String, String> getPartialLogs(String pid) 
    {
    	return logical.getPartialLogs(pid);
    }
    
    public String getPartialLogFile(String file) 
    {
    	return logical.getPartialLogFile(file);  	
    }
    
    public synchronized void startProcessConnector( int nPort) throws InterruptedException, IOException, ProcessConnectorException
    {
    	logical.startProcessConnectorFromMonitor( nPort);
    }
    
    public synchronized void checkProcessConnector() throws ProcessConnectorException, InterruptedException, IOException
    {
    	logical.checkProcessConnector();
    }
    
    public void replicateDB(String strDBName, DataHandler dhDBZip) throws Exception
    {
    	logical.replicateDB(strDBName, dhDBZip);    	
    }    
    
    public void synchronizeUpdate() throws ProcessConnectorException, InterruptedException 
    {
    	logical.synchronizeUpdate();
    }
    
    public String getProcessStatistics(String pid) throws ProcessConnectorException, InterruptedException 
    {
    	return logical.getProcessStatistics(pid);
    }
    
    public String asyncronousProcess(String[] params,String processName,String pid) throws Throwable 
    {
    	return logical.asyncronousProcess(params,processName,pid);
    }
    
    /*
    public void restart() throws Throwable {
    	TaskControllerLogical.getInstance().restart();
    }
    public void restartRoot() throws Throwable {
    	TaskControllerLogical.getInstance().restartRoot();
    }
    */
    
//    public void restartServices() throws Exception 
//    {
//    	logical.restartServices();
//    }
    
    public DataHandler requestAlerts( String strTimeStamp) throws IOException
    {
    	return logical.requestAlerts( strTimeStamp);
    }
    
	public boolean deleteAlert( String strAlertFile)
	{
		return logical.deleteAlert( strAlertFile);
	}
	
	public DataHandler requestProcessLogs(List<String> listPid, List<String> listDate, List<String> listType) throws IOException
	{
		return logical.requestProcessLogs( listPid, listDate, listType);
	}
	
	public DataHandler requestNodesLogs(List <String> listDates) throws IOException
    {
          return logical.requestNodesLogs(listDates);
    }
	
	public DataHandler requestPcLogs(List <String> listDates) throws IOException
    {
          return logical.requestPcLogs(listDates);
    }
	
	public DataHandler requestControlProcLogs(List <String> listDates) throws IOException
    {
          return logical.requestControlProcLogs(listDates);
    }

    public void launchPDMMonitor() throws ProcessConnectorException, InterruptedException 
    {
    	logical.launchPDMMonitor();
    }
    
    public boolean deleteCacheFolder(HashMap<String,ArrayList<String>> map) 
    {
		return logical.deleteCacheFolder(map);
	}
	
    public void replicateZipFile(DataHandler dhZipFile) throws TaskControllerException 
    {
    	logical.replicateZipFile(dhZipFile);
    }
    
    public String requestDeletedAlerts(String type, List <String> listFiles)
	{
		return logical.requestDeletedAlerts(type, listFiles);
	}
    
    public void stopNode() 
    {
    	logical.stopNode();
    }
    
    public String getXMLFileName(String filename) 
    {
    	return logical.getXMLFileName(filename);
    }
	
	public void killProcessConnector() throws Exception{
		logical.killProcessConnector();
	}
    
	public void stopModule() throws Throwable {
		instance = null;
		logical.stopModule();		
		logical = null;	
	}
	
	public void replicatePC(DataHandler dhPCZip) throws Exception
    {
    	logical.replicatePC(dhPCZip);    	
    }
}