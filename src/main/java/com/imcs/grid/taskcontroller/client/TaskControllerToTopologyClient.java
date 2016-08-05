package com.imcs.grid.taskcontroller.client;

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

/**
 * This class gathers all methods to communicate with Topology.
 */
public class TaskControllerToTopologyClient
{
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerToBrokerClient.class);
	private static String layer_service = GridConfiguration.getDefault().getParameter(ConfigurationParam.TOPOLOGY_SERVICE);
	
	private static final int trials= 2;
	private static final int interval_between_trials= 1500;
	private static String nodeGridName = Node.getMyself().getGridname();

	public static void checkAndStartUpDBIfNecessary() throws AxisFault, InterruptedException {
		GridConfiguration conf = GridConfiguration.getDefault();
		String location = conf.getParameter(ConfigurationParam.SC_BINDING_IP) + ":" + conf.getParameter(ConfigurationParam.SC_BINDING_PORT);

		OMElement message = TaskControllerCreateMessages.getCheckAndStartUpDBIfNecessaryMessage(nodeGridName);		
		EndpointReference epr = new EndpointReference("http://"+location+"/ws/services/"+layer_service);
		int i=0;
		while(i<trials) {
			try {
				String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + "checkAndStartUpDBIfNecessary";
				Client.clientBlocking(message, epr, false, action);
				i=100;
			} catch (AxisFault e) {
				logger.error("Error communicating with Topology - Num :: " + i,e);
				i++;
				if (i==2) throw e;
				Thread.sleep(interval_between_trials);
			}
		}
	}
	
	public static String getRootLocation() throws AxisFault, InterruptedException {
		GridConfiguration conf = GridConfiguration.getDefault();
		String location = conf.getParameter(ConfigurationParam.SC_BINDING_IP) + ":" + conf.getParameter(ConfigurationParam.SC_BINDING_PORT);
		
		OMElement message = TaskControllerCreateMessages.createGetRootLocationMessage(conf.getParameter(ConfigurationParam.ROOT_GRID_NAME), nodeGridName);
		EndpointReference epr = new EndpointReference("http://" + location + "/ws/services/" + layer_service);
		logger.debug(message);
		
		OMElement omResponse= null;
		int i=0;
		while(i<trials) {
			try {
				String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.GETROOT.getValue();
				omResponse= Client.clientBlocking(message, epr, false, action);
				i=100;
			} catch (AxisFault e) {
				logger.error("Error communicating with Topology - Num :: " + i,e);
				i++;
				if (i==2) throw e;
				Thread.sleep(interval_between_trials);
			}
		}		
		return TaskControllerReadMessages.getLocationText(omResponse);
	}
	
	/**Topology is keeping the state in every node. TaskController can communicate an event that changes
	 * the state (like PCResponse or PCTimeout).
	 * @param strEvent
	 * @throws AxisFault
	 * @throws InterruptedException
	 */
	public static void changeState( String strEvent) throws AxisFault, InterruptedException {
		GridConfiguration conf = GridConfiguration.getDefault();
		String location = conf.getParameter(ConfigurationParam.SC_BINDING_IP) + ":" + conf.getParameter(ConfigurationParam.SC_BINDING_PORT);

		OMElement message = TaskControllerCreateMessages.getCreateChangeStateMessage(strEvent, nodeGridName);
		EndpointReference epr = new EndpointReference("http://" + location + "/ws/services/" + layer_service);
		logger.debug(message);
		int i=0;
		while(i<trials) {
			try {
				String action = TaskcontrollerNS.WS_ACTION_PREFIX.getValue() + TaskcontrollerNS.CHANGESTATE.getValue();
				Client.clientBlocking(message, epr, false, action);
				i=100;
			} catch (AxisFault e) {
				logger.error("Error communicating with Topology - Num :: " + i,e);
				i++;
				if (i==2) throw e;
				Thread.sleep(interval_between_trials);
			}
		}		
	}
}
