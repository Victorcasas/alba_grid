package com.imcs.grid.taskcontroller.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.addressing.EndpointReference;

import com.imcs.grid.commons.ws.Client;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.taskcontroller.messages.TaskControllerCreateMessages;
import com.imcs.grid.taskcontroller.messages.TaskControllerReadMessages;
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerToBrokerClient {

	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerToBrokerClient.class);

	private static String services = "ws/services";
	private static String layer_service = "broker";
	private static String nodeGridName = Node.getMyself().getGridname();

	private static EndpointReference getRemoteService(String location, String layerService) {
		return new EndpointReference("http://" + location + "/" + services + "/" + layerService);
	}
	
	public static void notifyFinishToRoot(String pid) throws Exception {
		logger.info("----------------------------------------------------");
		logger.info("- Informing to the Root finish Job with pid :: " + pid);
		logger.info("----------------------------------------------------");

		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
//		String rootLocation = TaskControllerToTopologyClient.getRootLocation();
//
		OMElement message = TaskControllerCreateMessages.createFinishedJobMessage(pid, nodeGridName, Node.getMyself().getLocation());
		logger.debug(message);		
//		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		OMElement response;
		String action = "";
		EndpointReference epr;
		String rootLocation = "";
		
		int i=0;
		
		boolean send = false;
		while (!send) {
			try {
				rootLocation = TaskControllerToTopologyClient.getRootLocation();	
				epr = getRemoteService(rootLocation, layer_service);
				logger.info("Message FINISHED JOB TO " + epr);
				action =  TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.FINISHEDJOB.getValue();
				response = Client.clientBlocking(message, epr, false, action);
				send = TaskControllerReadMessages.getFinishJobResponse(response);
			} catch (Exception e) {
				logger.error("Error notifyFinishToRoot " + i,e);
				i++;
				Thread.sleep(1500);
			}
		}
		if (!send)
			throw new Exception("Error informing broker finish - Pid  :: " + pid );
	}

	public static void notifyChangePhaseToRoot(String pid, String newPhaseStatus)  throws Exception {
		logger.debug("-------------------------------------------------------------");
		logger.debug("- Informing to the Root change phase status of Job with pid :: " + pid + " ; Phase Status :: " + newPhaseStatus);
		logger.debug("-------------------------------------------------------------");

		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();
		
		OMElement message = TaskControllerCreateMessages.createChangeRunningPhaseMessage(pid, newPhaseStatus, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		logger.debug(message);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.CHANGERUNNINGPHASE.getValue();
		Client.clientBlocking(message, epr, false, action);
	}

	public static void deliver(String deliverTo, String pid, OMElement result) throws AxisFault	{
		OMElement message = TaskControllerCreateMessages.createDeliverMessage(pid, result, nodeGridName);
		EndpointReference epr = new EndpointReference(deliverTo);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.DELIVERRESULT.getValue();
		Client.clientOnlyInBlocking(message, epr, action);
	}
	
	public static void notifyChangeXmlToRoot() throws AxisFault, InterruptedException {
		logger.debug("------------------------------------");
		logger.debug("- Informing to the Root change XML -");
		logger.debug("------------------------------------");

		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String myLocation = GridConfiguration.getDefault().getParameter(ConfigurationParam.MY_LOCATION);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.createChangeXMLMessage(myLocation, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.CHANGEXML.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	 
	 public static void notifyStatusPCToRoot(String status) throws InterruptedException, AxisFault {
		logger.debug("----------------------------------------------------------------");
		logger.debug("- Informing to the Root status process connector :: " + status);
		logger.debug("----------------------------------------------------------------");

		GridConfiguration conf = GridConfiguration.getDefault();
		layer_service = conf.getParameter(ConfigurationParam.BROKER_SERVICE);
		String location = conf.getParameter(ConfigurationParam.SC_BINDING_IP) + ":" + conf.getParameter(ConfigurationParam.SC_BINDING_PORT);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.createStatusProcessConectorMessage(status, location, nodeGridName);	
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		logger.debug(message);
		
		int i=0;
		while (i < 2) {
			try {
				String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.STATUSPROCESSCONECTOR.getValue();
				Client.clientBlocking(message, epr, false, action);
				i=100;
			} catch (AxisFault e) {
				logger.error("Error notifying process connector status to Root - Num :: " + i,e);
				i++;
				if (i==2) throw e;
				Thread.sleep(1500);
			}
		}
	}
	 
	/**Gets all nodes within the grid. Used to find out which nodes are running on the same IP, in case it
	 * were necessary to start up the Process Connector.
	 * @return
	 * @throws InterruptedException, AxisFault
	 */
	public static List<Node> getAllNodes() throws InterruptedException, AxisFault {
		GridConfiguration conf = GridConfiguration.getDefault();
		layer_service = conf.getParameter(ConfigurationParam.BROKER_SERVICE);
		String location = conf.getParameter(ConfigurationParam.SC_BINDING_IP) + ":" + conf.getParameter(ConfigurationParam.SC_BINDING_PORT);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.createGetAllNodesMessage(location, nodeGridName);	
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		
		OMElement element= null;
		int i=0;
		while(i<2) {
			try {
				String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.GETALLNODES.getValue();
				element= Client.clientBlocking(message, epr, false, action);
				i=100;
			} catch (AxisFault e) {
				logger.error("Error getting all nodes" + i,e);
				i++;
				if (i==2) throw e;
				Thread.sleep(1500);
			}
		}
		List<Node> listNodes= new ArrayList<Node>();
		Iterator<?> it= element.getChildren();
		OMElement elIP = null;
		while(it.hasNext()) {
			elIP = (OMElement)it.next();
			listNodes.add(new Node( elIP.getText()));
		}	
		return listNodes;
	}
	
	public static void callAddStatisticsAlebra(String jcl, String step, String pid, String idExecution, double cpu_download,
			double elapsed_download, double cpu_upload, double elapsed_upload, int num_files_in, long size_files_in, int num_cache_files_in, 
			long size_cache_files_in, int num_files_out, long size_files_out, int redundance_code, ArrayList<String> files_sizes) throws AxisFault, 
			InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageAddStatisticsAlebra(jcl, step, pid, idExecution,
				truncateDouble(cpu_download), truncateDouble(elapsed_download), truncateDouble(cpu_upload),
				truncateDouble(elapsed_upload), num_files_in, size_files_in, num_cache_files_in, size_cache_files_in, 
				num_files_out, size_files_out, redundance_code, files_sizes, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ADDSTATISTICSALEBRA.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	
	private static double truncateDouble(double dNum) {
		double result = 0;
		if (dNum > 0)
			result = Math.floor(dNum * 100)/100;
		else
			result = Math.ceil(dNum * 100)/100;
		return result;
	}

	public static void callAddServiceExecution(String gridpid, String jcl,String step, String idNode, String idService, 
			String result, String rc, String rcDescription) throws AxisFault, InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageAddServiceExecution(gridpid, jcl, step, idNode, idService, 
				result, rc, rcDescription, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ADDSERVICEEXECUTION.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	
	public static void callAddResultParallel(String jcl,String step, String nameFile1, String nameFile2, String result, String fileCompareType,String rcActual,String rcDescActual) throws AxisFault, InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageAddResultParallel(jcl, step, nameFile1, nameFile2, result, nodeGridName,  fileCompareType, rcActual, rcDescActual);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ADDRESULTPARALLEL.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	
	public static void callEndExecution(String idExecution, String pidGrid, String endDate) throws AxisFault, InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageEndExecution(idExecution, pidGrid, endDate, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ENDEXECUTION.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	
	public static void callAddGridAvailability(String id, String node,
			Integer availableNodes, Long initTime, String service) throws AxisFault, InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageAddGridAvailability(id, node, availableNodes, initTime, service, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ADDGRIDAVAILABILITY.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
	
	public static void callAddExecution(String idExec, String exec, String idNode, String idSession,
			String idService, String pidGrid, String description, String initDate) throws AxisFault, InterruptedException {
		layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.BROKER_SERVICE);
		String rootLocation = TaskControllerToTopologyClient.getRootLocation();

		OMElement message = TaskControllerCreateMessages.getMessageAddExecution(idExec, exec, idNode, idSession, idService, pidGrid, description, initDate, nodeGridName);
		EndpointReference epr = getRemoteService(rootLocation, layer_service);
		String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.ADDEXECUTION.getValue();
		Client.clientBlocking(message, epr, false, action);
	}
}
