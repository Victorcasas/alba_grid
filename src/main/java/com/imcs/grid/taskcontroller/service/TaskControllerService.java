package com.imcs.grid.taskcontroller.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.activation.DataHandler;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.attachments.utils.DataHandlerUtils;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.OMText;

import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.error.KernelException;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.Service;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.client.TaskControllerToTopologyClient;
import com.imcs.grid.taskcontroller.logic.TaskControllerFrontal;
import com.imcs.grid.taskcontroller.messages.TaskControllerCreateMessages;
import com.imcs.grid.taskcontroller.messages.TaskControllerReadMessages;
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerService {

	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerService.class);
	private final String responsetag = "Response";
	
	private static TaskControllerFrontal frontal = null;
	
	public OMElement initModule(OMElement element) throws XMLStreamException {
		logger.info("InitModuleTaskcontroller - Message received --> " + element);
		OMElement omResponse = null;
		try {	
//			TaskControllerEventEngine.getInstance().init();
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			frontal = TaskControllerFrontal.getInstance();
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.INIT.getValue() +  responsetag, "ok");
		} catch (Exception e) {
			logger.error("Error initializing module taskController ",e);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.INIT.getValue() + responsetag, "ERROR = " + e.getMessage());
			System.exit(0);
		} finally {
			logger.info("InitModuleTaskcontroller END - Response -->" + omResponse);
		}
		return omResponse;
	}

	public OMElement performTask(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> \n\t\t" + element.toString());
		OMElement omResponse = TaskControllerCreateMessages.createOMResponse(TaskcontrollerNS.PERFORMTASK.getValue()); 
		OMNamespace omNs = TaskControllerCreateMessages.getOmNs();
		OMFactory fac = TaskControllerCreateMessages.getOmFac(); 
		try	{	
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			// The node must first check whether the job comes from the real root
			String strRootLocation= TaskControllerReadMessages.getRootLocation(element);
			String strMyRootLocation= TaskControllerToTopologyClient.getRootLocation();
			
			if (!strRootLocation.equals( strMyRootLocation)) {
				omResponse.addAttribute("errorCode", "1", omNs);
				throw new Exception( "Wrong root");			
			}
			OMElement gridjobMessage = TaskControllerCreateMessages.getGridJobMessage(element);
					
			String pid = frontal.performTask(gridjobMessage);
			if (!pid.equals("")) {
				OMElement value = fac.createOMElement("pgm", omNs);
				value.addAttribute("pid", pid, omNs);
				omResponse.addChild(value);
			} else {
				omResponse.setText("The node is executing a process. Job rejected.");
			}
		} catch(Throwable t) {
			logger.error("Serious irrecoverable error when executing job. Please inform.", t);
			omResponse.addAttribute("error", t.getMessage(), omNs);
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement getAllSubmittedMessages(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			List<OMElement> omMessages = frontal.getAllSubmittedMessages();
			omResponse = TaskControllerCreateMessages.getAllSubmitttedMessages(omMessages);
		} catch (Throwable e) {
			logger.error( "Serious irrecoverable error when getting all submitted messages.", e);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETALLSUBMITTEDMESSAGES.getValue() + responsetag, 
				"Serious irrecoverable error when getting all submitted messages. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement getSubmittedMessage(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String pid = TaskControllerReadMessages.getPidAttribute(element);
			omResponse = frontal.getSubmittedMessage(pid);
		} catch (TaskControllerException tcex) {
			logger.error(tcex.getMessage(), tcex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.GETSUBMITTEDMESSAGE.getValue() + responsetag,
					"ERROR = " + tcex.getMessage());
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting submitted message.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETSUBMITTEDMESSAGE.getValue() + responsetag,
					"Serious irrecoverable error when getting submitted message. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement monitor(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMFactory fac = OMAbstractFactory.getOMFactory();
		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/broker", "");
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			OMElement gridjob = element.getFirstElement();
			String totalTime = TaskControllerReadMessages.getTotalTimeText(element);
			String serviceTime = TaskControllerReadMessages.getServiceTimeText(element);
			//Si no viene <totalTime> o <serviceTime>, se devolver� la respuesta inmediatamente como se hac�a hasta ahora
			if (totalTime.equals("") || serviceTime.equals("")) {
				logger.info("Total Time :: "+totalTime+" Service Time :: "+serviceTime);
				totalTime = "100";
				serviceTime = "100";
			}
			omResponse = frontal.monitor(gridjob,totalTime,serviceTime);
		} catch(Throwable th) {
			logger.error("Error monitoring ", th);
			omResponse = fac.createOMElement("monitorResponse", omNs);
			fac.createOMText(omResponse, "Error " + th.getMessage());
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement closeSession(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String sessionId = TaskControllerReadMessages.getUniqueSessionId(element);
			frontal.closeSession(sessionId);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CLOSESESSION.getValue() + responsetag, "ok");
		} catch (KernelException kex) {
			logger.error(kex.getMessage(), kex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CLOSESESSION.getValue() + responsetag,
					"ERROR = " + kex.getMessage());
		} catch (InterruptedException iex) {
			logger.error(iex.getMessage(), iex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CLOSESESSION.getValue() + responsetag,
					"ERROR = " + iex.getMessage());
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when closing session.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.CLOSESESSION.getValue() + responsetag,
					"Serious irrecoverable error when closing session. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement allocate(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			OMElement gridjob = element.getFirstElement();
			omResponse= frontal.allocate("allocate", gridjob);
		} catch (Throwable e) {
			logger.error(" allocate error ", e);
			String problem = "No SessionId, eliminated by TIME OUT";
			omResponse = TaskControllerCreateMessages.getOMElementResponse("allocate", "", "error", problem, e.getMessage());
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	public OMElement htogrid(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMFactory fac = OMAbstractFactory.getOMFactory();
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/taskcontroller", "");
		OMNamespace omNs = fac.createOMNamespace("", "");
		OMElement response = fac.createOMElement("allocateResponse", omNs);
		try {
			OMElement gridjob = element.getFirstElement();
			response = frontal.allocate("htogrid", gridjob);
		} catch (Throwable e) {
			logger.error(" Allocate error ", e);
			fac.createOMText(response, "Error allocating files review log");
		} finally {
			logger.info("END - Response --> " + response);
		}
		return response;		
	}

	public OMElement getServicesID(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String[] services = frontal.getServicesID();
			omResponse = TaskControllerCreateMessages.getServicesMessage(TaskcontrollerNS.GETSERVICES.getValue() + responsetag, services);
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting services.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETSERVICES.getValue() + responsetag,
					"Serious irrecoverable error when getting services. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getServicesDescription(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String[] services = frontal.getDescriptionServices();
			omResponse = TaskControllerCreateMessages.getServicesDescriptionMessage(TaskcontrollerNS.GETSERVICESDESCRIPTION.getValue() + responsetag, services);
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting services description.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETSERVICESDESCRIPTION.getValue() + responsetag,
					"Serious irrecoverable error when getting services description. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement services(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			Collection<Service> services = frontal.getServicesInfo();
			omResponse = TaskControllerCreateMessages.getServicesInfoMessage(TaskcontrollerNS.SERVICES.getValue() + responsetag, services);
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting services information.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.SERVICES.getValue() + responsetag,
					"Serious irrecoverable error when getting services information. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}	
		return omResponse;
	}

	public OMElement getResult(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String pid = TaskControllerReadMessages.getPidAttribute(element);
			omResponse = frontal.getResult(pid);
		} catch (TaskControllerException tcex) {
			logger.error(tcex.getMessage(), tcex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.GETRESULT.getValue() + responsetag,
					"ERROR = " + tcex.getMessage());
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting result.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETRESULT.getValue() + responsetag,
					"Serious irrecoverable error when getting result. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

//	private static String getSessionId(OMElement message) throws XMLStreamException 
//	{
//		logger.debug("Message Received --> " + message);		
//		String sessionId = message.getAttributeValue(new QName("", "uniqueSessionId"));
//
//		if (UtilString.isNullOrEmpty(sessionId)) {
//			logger.error("sessionId NULL :: in closeSession");
//			throw new IllegalStateException("sessionId NULL :: in closeSession mess::: " + message);
//		}
//		return sessionId;
//	}

	public OMElement getActivePids(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		Collection<String> pids = frontal.getActivePids();
		OMElement omResponse = TaskControllerCreateMessages.getActivePidsMessage(TaskcontrollerNS.GETACTIVEPIDS.getValue() + responsetag, pids);
		logger.info("END - Response --> " + omResponse);
		return omResponse; 
	}

	public OMElement changeXml(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String operation = TaskControllerReadMessages.getOperationAttribute(element.getFirstElement());
			frontal.changeXML(operation);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CHANGEXML.getValue() + responsetag, "ok");
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when changing XML.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.CHANGEXML.getValue() + responsetag,
					"Serious irrecoverable error when changing XML. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getXMLDocument(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
    		if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String document = TaskControllerReadMessages.getDocumentAttribute(element.getFirstElement());
			logger.debug("Document :: " + document);
			omResponse = frontal.getXMLDocument(document);
		} catch (TaskControllerException tcex) {
			logger.error(tcex.getMessage(), tcex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.GETXMLDOCUMENT.getValue() + responsetag,
					"ERROR = " + tcex.getMessage());
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting XML document.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETXMLDOCUMENT.getValue() + responsetag,
					"Serious irrecoverable error when getting XML document. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
    }
    
    public OMElement setXMLDocument(OMElement element) throws XMLStreamException {
    	logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
    	try {
    		String gridName = TaskControllerReadMessages.getGridNameText(element);
    		if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
    		String title = TaskControllerReadMessages.getTitleText(element);
			OMElement contentElement = TaskControllerCreateMessages.getContentOMElement(element);
			String response = frontal.setXMLDocument(title, contentElement.getText());
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.SETXMLDOCUMENT.getValue() + responsetag, response);
		} catch (TaskControllerException tcex) {
			logger.error(tcex.getMessage(), tcex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.SETXMLDOCUMENT.getValue() + responsetag,
					"ERROR = " + tcex.getMessage());
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when setting XML document.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.SETXMLDOCUMENT.getValue() + responsetag,
					"Serious irrecoverable error when setting XML document. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
    }
    
	public OMElement changeTopologyConfig(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
    	try {
    		String nodegridName = TaskControllerReadMessages.getGridNameText(element);
    		//If gridName = "" is because it still has not been established
    		if (!Node.getMyself().getGridname().equals("") && !nodegridName.equals("") && !nodegridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+nodegridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+nodegridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
    		String newRootLocation = TaskControllerReadMessages.getNewRootLocationAttribute(element);
    		String gridName = TaskControllerReadMessages.getGridNameAttribute(element);
    		logger.debug("New root location :: " + newRootLocation + " *** Grid name :: " + gridName);
    		frontal.changeTopologyConfig(newRootLocation, gridName);
    		omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CHANGETOPOLOGYCONFIG.getValue() + responsetag, "ok");
    	} catch (Throwable t) {
			logger.error("Serious irrecoverable error when changing topology configuration.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.CHANGETOPOLOGYCONFIG.getValue() + responsetag,
					"Serious irrecoverable error when changing topology configuration. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getPartialLogs(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String pid = TaskControllerReadMessages.getPidAttribute(element.getFirstElement()); 
			logger.debug("PID :: " + pid);
			Map<String, String> partialLogs = frontal.getPartialLogs(pid);
			omResponse = TaskControllerCreateMessages.getPartialLogsMessage(TaskcontrollerNS.GETPARTIALLOGS.getValue(), partialLogs);
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting partial logs.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETPARTIALLOGS.getValue() + responsetag,
					"Serious irrecoverable error when getting partial logs. Check logs and inform your dealer.");
		} finally {
			logger.debug("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getPartialLogFile(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String file = element.getAttributeValue(new QName("file"));
			logger.debug("File :: " + file);
			String response = frontal.getPartialLogFile(file);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.GETPARTIALLOGFILE.getValue(), response);
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting partial log file.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.GETPARTIALLOGS.getValue() + responsetag,
					"Serious irrecoverable error when getting partial log file. Check logs and inform your dealer.");
		} finally {
			logger.debug("END - Response --> " + omResponse);
		}
	
		return omResponse;
	}
	
	/**Asks this node to start the Process Connector. It is supposed that a PC cannot be started up if 
	 * there is already another one running. Use only from monitor.
	 * @param element contains the tag "port" with the number of the port that PC will be listening to.
	 * @return ERROR if PC was not started up, or there was a PC already working. 
	 */
	public OMElement startProcessConnector( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		int nPort= -1;
		OMElement omResponse= null;
		try {
			nPort= Integer.parseInt( TaskControllerReadMessages.getPortAttribute(element));
			logger.debug("Port :: " + nPort);
			frontal.startProcessConnector( nPort);
			MdsProvider.writeAlert( "A new instance of the Process Connector has been created in port "+nPort+".","infomsg_" + System.currentTimeMillis());					
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.STARTPROCESSCONNECTOR.getValue(), "OK");
		} catch ( ProcessConnectorException e) {
			logger.error( e.getMessage(), e);
			MdsProvider.writeAlert( e.getMessage(),"errormsg_" + System.currentTimeMillis());
			omResponse= TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.STARTPROCESSCONNECTOR.getValue(), "ERROR");
		} catch( Exception e) {
			logger.error( e.getMessage(), e);
			MdsProvider.writeAlert( "Serious irrecoverable error when attemping to start PC up. Check logs and inform dealer.","errormsg_" + System.currentTimeMillis());
			omResponse= TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.STARTPROCESSCONNECTOR.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	/**Sometimes the PC doesn't respond in time, and the TaskControllerThread ends setting the state of the node to BLACK.
	 * This method is called from the pop up menu in the monitor. Starts a new checking thread, just in case
	 * the PC is really working.
	 * @param element
	 * @return
	 */
	public OMElement checkProcessConnector( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse= null;
		try	{
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			frontal.checkProcessConnector();
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.CHECKPROCESSCONNECTOR.getValue(), "OK");
		} catch( ProcessConnectorException e) {
			logger.error( e.getMessage(), e);
			omResponse= TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CHECKPROCESSCONNECTOR.getValue(), "ERROR");
		} catch( Exception e) {
			logger.error( "Serious irrecoverable error. Please inform your dealer.", e);
			omResponse= TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.CHECKPROCESSCONNECTOR.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}

	/**Receives a zip containing a copy of a DB. This copy must be unzipped and moved to its corresponding directory.
	 * The name of every possible replicated DB and the directory must be specified in the .xml config files.
	 * @param element contains the name of the DB.
	 * @return ERROR if the database was not correctly updated.
	 */
	public OMElement replicateDB( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse= null;
		String strDBName= TaskControllerReadMessages.getNameAttribute(element);
		try	{
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			OMText omDBZip= (OMText)element.getFirstOMChild();
			DataHandler dhDBZip= ( DataHandler)DataHandlerUtils.getDataHandlerFromText( omDBZip.getText(), "application/x-gzip");
			frontal.replicateDB( strDBName, dhDBZip);
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.PROPAGATEDB.getValue(), "OK");
		} catch( Exception e) {
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.PROPAGATEDB.getValue(), "ERROR");
			logger.error( "Serious irrecoverable error when replicating DB "+strDBName+" to this node. Inform your dealer.", e);
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement synchronizeUpdate(OMElement element) throws XMLStreamException	{
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			frontal.synchronizeUpdate();
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.SYNCHRONIZEUPDATE.getValue() + responsetag, "OK");
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when synchronizing update.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.SYNCHRONIZEUPDATE.getValue() + responsetag,
					"Serious irrecoverable error when synchronizing update. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getProcessStatistics(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String pid = TaskControllerReadMessages.getPidAttribute(element);
			String statistics = frontal.getProcessStatistics(pid);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.PROCESSSTATISTICS.getValue(), statistics);
		} catch (ProcessConnectorException e) {
			logger.error( e.getMessage(), e);
			omResponse= TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.PROCESSSTATISTICS.getValue(), "ERROR");
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when getting process statistics.", t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.PROCESSSTATISTICS.getValue() + responsetag,
					"Serious irrecoverable error when getting process statistics. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement asyncronousProcess(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String processName = TaskControllerReadMessages.getCmdAttribute(element);
			String pid = "asyncronousProcess_" + System.currentTimeMillis();
			logger.debug("pid::" + pid);
			String[] params = TaskControllerReadMessages.getParamsAttribute(element).split(";");
			String response = frontal.asyncronousProcess(params,processName,pid);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.ASYNCRONOUSPROCESS.getValue(), response);
		} catch (Throwable t) {
			logger.error("Error during asyncronous process",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.ASYNCRONOUSPROCESS.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement requestAlerts(OMElement element) throws XMLStreamException	{
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String strTimeStamp = TaskControllerReadMessages.getTimeAttribute(element);
			DataHandler dhAlertFiles= frontal.requestAlerts( strTimeStamp);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTALERTS.getValue(), dhAlertFiles);
		} catch (Throwable t) {
			logger.error("Error when delivering alerts",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTALERTS.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement deleteAlert( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse= null;
		try	{
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			String strFile= TaskControllerReadMessages.getFileText(element); 		
			boolean bOk= frontal.deleteAlert(strFile);
			omResponse = TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.DELETEALERT.getValue(),
					bOk ? "OK" : "Alert deleted in root, but fail to delete alert remotely in node.");
		} catch( Exception e3) {
			logger.error( "Serious irrecoverable error when deleting alerts.", e3);
			omResponse = TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.DELETEALERT.getValue(),
				"Serious irrecoverable error when deleting alerts. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement requestProcessLogs(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			List <String> listPid= new ArrayList<String>();
			List <String> listDate= new ArrayList<String>();
			List <String> listType= new ArrayList<String>();
			Iterator <?> itPid = element.getChildrenWithName(new QName ("",TaskcontrollerNS.PID.getValue()));
			Iterator <?> itDate = element.getChildrenWithName(new QName ("",TaskcontrollerNS.DATE.getValue()));
			Iterator <?> itType = element.getChildrenWithName(new QName ("",TaskcontrollerNS.TYPE.getValue()));
			OMElement pid, date, type;
			while(itPid.hasNext()){
				pid = (OMElement) itPid.next();
				listPid.add(pid.getText());
			}
			while(itDate.hasNext()){
				date = (OMElement) itDate.next();
				listDate.add(date.getText());
			}
			while(itType.hasNext()){
				type = (OMElement) itType.next();
				listType.add(type.getText());
			}
			DataHandler dhLogFiles= frontal.requestProcessLogs(listPid, listDate, listType);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTPROCESSLOGS.getValue(), dhLogFiles);
		} catch (Throwable t) {
			logger.error("Error when delivering logs",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTPROCESSLOGS.getValue(), "ERROR");
		} finally {
			logger.debug("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement requestNodeLogs(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
        	if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
        		logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
        	  	throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
        	}
        	List <String> listDates = TaskControllerReadMessages.getDatesText(element);
                
        	DataHandler dhLogFiles= frontal.requestNodesLogs(listDates);
               
            omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTNODELOGS.getValue(), dhLogFiles);
            logger.info("END - Response --> OK");
		} catch (Throwable t) {
			logger.error("Error in get logs",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTNODELOGS.getValue(), "ERROR");
		}
		return omResponse;
	}
	
	public OMElement requestPcLogs(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			List <String> listDates = TaskControllerReadMessages.getDatesText(element);
                
			DataHandler dhLogFiles= frontal.requestPcLogs(listDates);
			
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTPCLOGS.getValue(), dhLogFiles);
			logger.info("END - Response --> OK");
		} catch (Throwable t) {
			logger.error("Error in get logs",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTPCLOGS.getValue(), "ERROR");
		}	
		return omResponse;
	}
	
	public OMElement requestControlProcLogs(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
				logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
				throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
			}
			List <String> listDates = TaskControllerReadMessages.getDatesText(element);
                
			DataHandler dhLogFiles= frontal.requestControlProcLogs(listDates);
               
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTCONTROLPROCLOGS.getValue(), dhLogFiles);
			logger.info("END - Response --> OK");
		} catch (Throwable t) {
			logger.error("Error in get logs",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.REQUESTCONTROLPROCLOGS.getValue(), "ERROR");
		}
		return omResponse;
	}
	
	/*public OMElement restart(OMElement element){
		try{
			logger.info("Begin Restart process");
			TaskControllerFrontal.getInstance().restart();
		}catch (Throwable t){ 
				logger.error("Error during restart process",t);
		}
		OMElement omResponse = getResponseOMElement(GridNameSpace.RESTART.getValue(), "OK");
		return omResponse;
	}
	
	public OMElement restartRoot(OMElement element){
		try{
			TaskControllerFrontal.getInstance().restartRoot();
		}catch (Throwable t){ 
			logger.error("Error during restart root process",t);
		}
		OMElement omResponse = getResponseOMElement(GridNameSpace.RESTART.getValue(), "OK");
		return omResponse;
	}*/
	
	
	/**
	 * Service launched to initialize broker to monitor PDM
	 * Message: <launchPDMMonitor />
	 * Return:
	 * 			<launchPDMMonitor><text>OK</text></launchPDMMonitor>
	 * 			or
	 * 			<launchPDMMonitor><text>ERROR</text></launchPDMMonitor>
	 */
	public OMElement launchPDMMonitor(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			frontal.launchPDMMonitor();
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.LAUNCHPDMMONITOR.getValue(), "OK");
		} catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with process connector to launch PDM monitor.", pcex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.LAUNCHPDMMONITOR.getValue(), "ERROR");
		} catch (Exception ex) {
			logger.error("Error to launch PDM monitor from taskcontroller.", ex);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.LAUNCHPDMMONITOR.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement deleteCacheFolder(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			HashMap<String, ArrayList<String>> map = TaskControllerReadMessages.getFoldersAndExtensions(element);
						
			boolean delOk = frontal.deleteCacheFolder(map);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.DELETECACHEFOLDER.getValue(), 
					delOk ? "OK" : "KO");
		} catch (Throwable t) {
			logger.error("Error when deleting cache folder",t);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.DELETECACHEFOLDER.getValue(), "ERROR");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	/**Receives a zip containing a files. These files must be unzipped and moved to its corresponding directory.
	 * @return ERROR if the files could not be unzipped.
	 */
	public OMElement replicateZipFile( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try	{
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			OMText omZipFile = (OMText)element.getFirstOMChild();
			DataHandler dhZipFile = (DataHandler)DataHandlerUtils.getDataHandlerFromText( omZipFile.getText(), "application/zip");
			frontal.replicateZipFile(dhZipFile);
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.REPLICATEZIPFILE.getValue(), "OK");
		} catch( Exception e) {
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.REPLICATEZIPFILE.getValue(), "ERROR");
			logger.error( "Serious irrecoverable error when replicating Zip file to this node. Inform your dealer.", e);
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	// Reemplazado por shutdownKillNode
	public OMElement stopNode(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message Received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			frontal.stopNode();
			/* You can never reach the next line of code */
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.STOPNODE.getValue() + responsetag, "ok");
		} catch(Exception e) {
			logger.error( "Serious irrecoverable error when stopping node.", e); 
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.STOPNODE.getValue() + responsetag,
				"Serious irrecoverable error when stopping node. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement getFileNameTcFlow(OMElement element) throws XMLStreamException {
		logger.info("INIT - Message Received --> " + element);
		OMElement omResponse = null;
		try {
			String document = TaskControllerReadMessages.getDocumentAttribute(element.getFirstElement());
			String filename = frontal.getXMLFileName(document);
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.FILE.getValue() + responsetag, filename);
		} catch(Exception e) {
			logger.error( "Serious irrecoverable error when get filename taskcontroller flow.", e); 
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.FILE.getValue() + responsetag,
				"Serious irrecoverable error when getting filename taskcontroller flow. Check logs and inform your dealer.");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement killProcessConnector(OMElement element) {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			//If gridName = "" is because it has been established before killing process connector
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			frontal.killProcessConnector();
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.KILL_PC.getValue() + responsetag, "ok");
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when kill process connector process.",t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.KILL_PC.getValue() + responsetag,
					"Serious irrecoverable error when kill process connector process");
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
	
	public OMElement stopModule(OMElement element) {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse = null;
		try {
//			TaskControllerEventEngine.getInstance().stop();
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			frontal.stopModule();
			omResponse = TaskControllerCreateMessages.getResponseOMElement(TaskcontrollerNS.STOP.getValue() + responsetag, "ok");
		} catch (Throwable t) {
			logger.error("Serious irrecoverable error when stop topology module.",t);
			omResponse = TaskControllerCreateMessages.getErrorResponseOMElement(TaskcontrollerNS.STOP.getValue() + responsetag,
					"Serious irrecoverable error when stop topology module.");
		} finally {
			frontal = null;				
		}
		return omResponse;
	}
	
	/**Receives a txt containing a copy of a PC parms file.
	 * @return ERROR if the database was not correctly updated.
	 */
	public OMElement replicatePC( OMElement element) throws XMLStreamException {
		logger.info("INIT - Message received --> " + element);
		OMElement omResponse= null;
		try	{
			String gridName = TaskControllerReadMessages.getGridNameText(element);
			if (!Node.getMyself().getGridname().equals("") && !gridName.equals("") && !gridName.equals(Node.getMyself().getGridname())) {
  	  			logger.error("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname());
  	  			throw new TaskControllerException("Message gridname: "+gridName+" does not match node gridname: "+Node.getMyself().getGridname(), ErrorType.ERROR);
  	  		}
			OMText omPCZip= (OMText)element.getFirstOMChild();
			DataHandler dhPCZip= ( DataHandler)DataHandlerUtils.getDataHandlerFromText( omPCZip.getText(), "text/plain");
			frontal.replicatePC(dhPCZip);
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.PROPAGATEPC.getValue(), "OK");
		} catch( Exception e) {
			omResponse= TaskControllerCreateMessages.getResponseOMElement( TaskcontrollerNS.PROPAGATEPC.getValue(), "ERROR");
			logger.error( "Serious irrecoverable error when replicating PC parms file to this node. Inform your dealer.", e);
		} finally {
			logger.info("END - Response --> " + omResponse);
		}
		return omResponse;
	}
}