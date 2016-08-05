package com.imcs.grid.taskcontroller.messages;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.activation.DataHandler;
import javax.xml.namespace.QName;
import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;
import org.apache.axiom.om.OMText;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;

import com.imcs.grid.taskcontroller.Service;
import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerCreateMessages {

	private static OMFactory fac = OMAbstractFactory.getOMFactory();
//	private static OMNamespace emptyomns = fac.createOMNamespace("","");
//	private static OMNamespace omNs = fac.createOMNamespace(TaskcontrollerNS.TASKCONTROLLERNS.getValue(), "");
	private static OMNamespace omNs = fac.createOMNamespace("", "");
//	private static String prefix = "";
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerCreateMessages.class);
	
	public static OMNamespace getOmNs() {
		return omNs;
	}
	
	public static OMFactory getOmFac() {
		return fac;
	}
	
	public static OMElement getGridJobMessage(OMElement message) {
		StAXOMBuilder builder = new StAXOMBuilder(message.getXMLStreamReader());
		OMElement performTask = builder.getDocumentElement();
		OMElement gridjobMessage = performTask.getFirstChildWithName(new QName("", TaskcontrollerNS.GRIDJOB.getValue()));
		return gridjobMessage;
	}
	
	public static OMElement getSubmitttedMessage(OMElement message, String status, String phaseStatus) {
		message.addAttribute(fac.createOMAttribute(TaskcontrollerNS.STATUS.getValue(), omNs, status));
		message.addAttribute(fac.createOMAttribute(TaskcontrollerNS.PHASESTATUS.getValue(), omNs, phaseStatus));
		return message;
	}

	public static OMElement getAllSubmitttedMessages(List<OMElement> messages) {
		OMElement gridJobListElto = fac.createOMElement(TaskcontrollerNS.GRIDJOBS.getValue(),omNs);
		for(OMElement mess : messages)
			gridJobListElto.addChild(mess);
		return gridJobListElto;
	}
	
	public static OMElement getFoldersOMElement(String operation, String message, List<?> arrDirs, List<?> arrExts) {
		OMElement method = fac.createOMElement(operation, omNs);
		OMElement value = fac.createOMElement(TaskcontrollerNS.TEXT.getValue(), omNs);
		value.addChild(fac.createOMText(value, message));
		method.addChild(value);
		value = fac.createOMElement(TaskcontrollerNS.FOLDERS.getValue(), omNs);
		
		Iterator<?> itDir = arrDirs.iterator();
		Iterator<?> itExt = arrExts.iterator();
		String dir="", ext="";
		OMElement fold;
		while (itDir.hasNext())	{
			dir = (String) itDir.next();
			ext = (String) itExt.next();

			fold = fac.createOMElement(TaskcontrollerNS.FOLDER.getValue(), omNs);
			fold.addAttribute(TaskcontrollerNS.DIRECTORYNAME.getValue(), dir, omNs);
			fold.addAttribute(TaskcontrollerNS.EXTENSIONFILES.getValue(), ext, omNs);
			value.addChild(fold);
		}
		method.addChild(value);
		return method;
	}
	
	public static OMElement getResponseOMElement(String response, String message) {
		OMElement method = fac.createOMElement(response, omNs);
		OMElement value = fac.createOMElement(TaskcontrollerNS.TEXT.getValue(), omNs);
		value.addChild(fac.createOMText(value, message));
		method.addChild(value);
		return method;
	}
	
	public static OMElement getResponseOMElement(String response, DataHandler dataFile) {
		OMElement method = fac.createOMElement(response, omNs);
		method.addAttribute(TaskcontrollerNS.EMPTY.getValue(), dataFile==null ? "y" : "n", omNs);
		if (dataFile!=null) {
			OMText expectedTextData = fac.createOMText(dataFile, true);
			method.addChild(expectedTextData);
		}
		return method;
	}
	
	public static OMElement createOMResponse(String response) {
		OMElement omResponse = fac.createOMElement(response, omNs);
		return omResponse;
	}
	
	public static OMElement getResponseWithAttribute(String response, String attribute, String value) {
		OMElement omResponse = fac.createOMElement(response, omNs);
		omResponse.addAttribute(attribute, value, omNs);
		return omResponse;
	}
	
	public static OMElement getErrorResponseOMElement(String responseTag, String errorDesc) {
		OMElement method = fac.createOMElement(responseTag, omNs);
		method.addAttribute("error","true", omNs);
		fac.createOMText(method,errorDesc);
		return method;
	}
	
	public static OMElement getOMElementResponse(String service, String jcl, String step, String id, String time,
			   String age, String state, String description, String result, FileInfo[] fileOutputs, OMElement extra) {
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement gridjobResponse = factory.createOMElement(TaskcontrollerNS.GRIDJOB_RESPONSE.getValue(), name);
	
		if(!UtilString.isNullOrEmpty(jcl) || !(UtilString.isNullOrEmpty(step)))	{
			OMElement mvsInfoResponse = factory.createOMElement(TaskcontrollerNS.MVSINFO.getValue(), name);
			if (!UtilString.isNullOrEmpty(jcl))
				mvsInfoResponse.addAttribute(TaskcontrollerNS.JCL.getValue(), jcl, name);
			if (!UtilString.isNullOrEmpty(jcl))
				mvsInfoResponse.addAttribute(TaskcontrollerNS.STEP.getValue(), step, name);
			gridjobResponse.addChild(mvsInfoResponse);
		}
		OMElement pgmResponse = factory.createOMElement(TaskcontrollerNS.PGM.getValue(), name);
		if (!UtilString.isNullOrEmpty(id))
			pgmResponse.addAttribute(TaskcontrollerNS.ID.getValue(), id, name);
		gridjobResponse.addChild(pgmResponse);
	
		OMElement nodeInfoResponse = factory.createOMElement(TaskcontrollerNS.NODE_INFO.getValue(), name);
		if (!UtilString.isNullOrEmpty(time))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.TIME.getValue(), time, name);
	
		if (state.equals("wait_for_prealloc") && fileOutputs == null)
			nodeInfoResponse.addAttribute(TaskcontrollerNS.STATE.getValue(), "running", name);
		else if (!UtilString.isNullOrEmpty(state))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.STATE.getValue(), state, name);
	
		if (!UtilString.isNullOrEmpty(description))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.DESCRIPTION.getValue(), description, name);
		if (!UtilString.isNullOrEmpty(description))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.AGE.getValue(), description, name);        
	
		gridjobResponse.addChild(nodeInfoResponse);
	
		if ((!UtilString.isNullOrEmpty(result))) {
			OMElement resultResponse = factory.createOMElement(TaskcontrollerNS.RESULT.getValue(), name);
			resultResponse.setText(result);
			nodeInfoResponse.addChild(resultResponse);
		}  
	
		if (state.equals("wait_for_prealloc") && fileOutputs != null) {
			OMElement file, wait = factory.createOMElement(TaskcontrollerNS.WAIT_FOR_PREALLOC.getValue(), name);
			
			for (int i=0, iUntil=fileOutputs.length; i < iUntil; i++) {
				file = factory.createOMElement(TaskcontrollerNS.FILE.getValue(), name);
				file.addAttribute(TaskcontrollerNS.NAME.getValue(), fileOutputs[i].getName(), name);
				file.addAttribute(TaskcontrollerNS.SIZE.getValue(), String.valueOf(fileOutputs[i].getSize()), name);
				wait.addChild(file);
			} 
			gridjobResponse.addChild(wait);
		} 
		if (extra != null) 
			gridjobResponse.addChild(extra);
	
		return gridjobResponse;
	}
	
	public static OMElement getServicesMessage(String response, String[] services) {
		OMElement omService, omMessage = fac.createOMElement(response, omNs);
		for (int i=0, iUntil=services.length; i < iUntil; i++) {
			omService = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
			omService.addAttribute(TaskcontrollerNS.ID.getValue(), services[i], omNs);
			omMessage.addChild(omService);
		}
		return omMessage;
	}
	
	public static OMElement getServicesDescriptionMessage(String response, String[] services) {
		OMElement omService, omMessage = fac.createOMElement(response, omNs);
		for (int i=0, iUntil=services.length; i < iUntil; i++) {
			omService = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
			omService.addAttribute(TaskcontrollerNS.DESC.getValue(), services[i], omNs);
			omMessage.addChild(omService);
		}
		return omMessage;
	}
	
	public static OMElement getServicesInfoMessage(String response, Collection<Service> services) {
		OMElement omService, omMessage = fac.createOMElement(response, omNs);
		Iterator<Service> itServices = services.iterator();
		Service service;
		while ( itServices.hasNext()) {	
			service = itServices.next();
			omService = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
			omService.addAttribute(TaskcontrollerNS.ID.getValue(), service.getId(), omNs);
			omService.addAttribute(TaskcontrollerNS.DESC.getValue(), service.getDescription(), omNs);
			omService.addAttribute(TaskcontrollerNS.SUMMARY.getValue(), service.getSummary(), omNs);
			omMessage.addChild(omService);
		}
		return omMessage;
	}
	
	public static OMElement getActivePidsMessage(String response, Collection<String> pids) {
		OMElement pidElto, omMessage = fac.createOMElement(response, omNs);
		for (String pid : pids) {
			pidElto = fac.createOMElement(TaskcontrollerNS.PID.getValue(), omNs);
			pidElto.addAttribute(fac.createOMAttribute(TaskcontrollerNS.VALUE.getValue(), omNs, pid));
			omMessage.addChild(pidElto);
		}
		return omMessage;
	}
	
	public static OMElement getOMElementResponse(String service, String id, String state, String description, String result) {
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement gridjobResponse = factory.createOMElement(TaskcontrollerNS.GRIDJOB_RESPONSE.getValue(), name);

		OMElement pgmResponse = factory.createOMElement(TaskcontrollerNS.PGM.getValue(), name);
		if (!UtilString.isNullOrEmpty(id))
			pgmResponse.addAttribute(TaskcontrollerNS.ID.getValue(), id, name);
		gridjobResponse.addChild(pgmResponse);

		OMElement nodeInfoResponse = factory.createOMElement(TaskcontrollerNS.NODE_INFO.getValue(), name);

		if (!UtilString.isNullOrEmpty(description))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.DESCRIPTION.getValue(), description, name);
		if (!UtilString.isNullOrEmpty(description))
			nodeInfoResponse.addAttribute(TaskcontrollerNS.AGE.getValue(), "0", name);        

		gridjobResponse.addChild(nodeInfoResponse);

		if ((!UtilString.isNullOrEmpty(result))) {
			OMElement resultResponse = factory.createOMElement(TaskcontrollerNS.RESULT.getValue(), name);
			resultResponse.setText(result);
			nodeInfoResponse.addChild(resultResponse);
		}  
		return gridjobResponse;
	}
	
	public static OMElement createFinishedJobMessage(String pid, String gridname, String nodeLocation) {
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/broker", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.FINISHEDJOB.getValue(), omNs);
		OMElement idElem = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		idElem.addChild(fac.createOMText(idElem, pid)); 
		message.addChild(idElem);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		OMElement nodeLocationElem = fac.createOMElement(TaskcontrollerNS.LOCATION.getValue(), omNs);
		nodeLocationElem.addChild(fac.createOMText(nodeLocationElem, nodeLocation));
		message.addChild(nodeLocationElem);
		return message;
	}
	
	public static OMElement createChangeRunningPhaseMessage(String pid, String newPhaseStatus, String gridname)	{
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/broker", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.CHANGERUNNINGPHASE.getValue(), omNs);
		OMElement phaseStatus = fac.createOMElement(TaskcontrollerNS.PHASESTATUS.getValue(), omNs);
		phaseStatus.addChild(fac.createOMText(phaseStatus, newPhaseStatus));
		OMElement idElem = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		idElem.addChild(fac.createOMText(idElem, pid)); 
		message.addChild(idElem);
		message.addChild(phaseStatus);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;
	}
	
	public static OMElement createDeliverMessage(String pid, OMElement result, String gridname) {
		OMElement message = fac.createOMElement(TaskcontrollerNS.DELIVERRESULT.getValue(), omNs);
		message.addAttribute(fac.createOMAttribute(TaskcontrollerNS.PID.getValue(),omNs,pid));
		message.addChild(result);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;
	}
	
	public static OMElement createChangeXMLMessage(String myLocation, String gridname) {
		OMElement message = fac.createOMElement(TaskcontrollerNS.CHANGEXML.getValue(), omNs);
		OMElement changeXmlGrid = fac.createOMElement(TaskcontrollerNS.CHANGEXMLGRID.getValue(), omNs);
		changeXmlGrid.addAttribute(TaskcontrollerNS.OPERATION.getValue(), "restartServices", omNs);
		changeXmlGrid.addAttribute(TaskcontrollerNS.ADDR.getValue(), myLocation, omNs);
		message.addChild(changeXmlGrid);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;        
	}
	
	 public static OMElement getOMElementString(String name) throws XMLStreamException, FileNotFoundException {
        StAXOMBuilder builder= new StAXOMBuilder(new ByteArrayInputStream(name.getBytes()));
        OMElement message = builder.getDocumentElement();
        StringWriter writer = new StringWriter();
        try {
        	message.serialize(XMLOutputFactory.newInstance().createXMLStreamWriter(writer));
		} catch (XMLStreamException e) {
			logger.error("Error getting OMElement String",e);
		} catch (FactoryConfigurationError e) {
			logger.error("Error getting OMElement String",e);
		}
        writer.flush();
        return message;
	}
	 
	 public static OMElement createStatusProcessConectorMessage(String status, String location, String gridname) {
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/broker", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.STATUSPROCESSCONECTOR.getValue(), omNs);
		OMElement locationElem = fac.createOMElement(TaskcontrollerNS.LOCATION.getValue(), omNs);
		locationElem.addChild(fac.createOMText(locationElem, location)); 
		message.addChild(locationElem);	
		OMElement statusElem = fac.createOMElement(TaskcontrollerNS.STATUS.getValue(), omNs);
		statusElem.addChild(fac.createOMText(statusElem, status)); 
		message.addChild(statusElem);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;
	 }
	 
	 public static OMElement createGetAllNodesMessage(String location, String gridname) {
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/broker", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.GETALLNODES.getValue(), omNs);
		OMElement locationElem = fac.createOMElement(TaskcontrollerNS.LOCATION.getValue(), omNs);
		locationElem.addChild(fac.createOMText(locationElem, location)); 
		message.addChild(locationElem);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;
	 }
	 
	 public static OMElement createGetRootLocationMessage(String gridname, String nodegridname) {
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/topology", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.GETROOT.getValue(), omNs);
		message.addAttribute("gridName", gridname, omNs);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, nodegridname));
		message.addChild(gridnameElem);
		return message;		
	 }
	 
	 public static OMElement getCreateChangeStateMessage(String event, String gridname) {
//		OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/topology", "");
		OMElement message = fac.createOMElement(TaskcontrollerNS.CHANGESTATE.getValue(), omNs);
		message.addAttribute(TaskcontrollerNS.EVENT.getValue(), event, omNs);
		OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		message.addChild(gridnameElem);
		return message;
	 }
	 
	 public static OMElement getContentOMElement(OMElement message) {
		 return message.getFirstChildWithName(new QName(TaskcontrollerNS.CONTENT.getValue()));
	 }
	 
	 public static OMElement getPartialLogsMessage(String response, Map<String, String> partialLogs) {
		 OMElement message = fac.createOMElement(response, omNs);
		 OMElement logs = fac.createOMElement(TaskcontrollerNS.LOGS.getValue(), omNs);
		 OMElement files = fac.createOMElement(TaskcontrollerNS.FILES.getValue(), omNs);
		 OMElement fileContent = fac.createOMElement(TaskcontrollerNS.FILECONTENT.getValue(), omNs);
		 
		 Iterator<String> filenames = partialLogs.keySet().iterator();
		 String strContent = "", filename="";
		 OMElement file;
		 while (filenames.hasNext()) {
			 filename = filenames.next();
			 file = fac.createOMElement(TaskcontrollerNS.FILE.getValue(), omNs);
			 file.addAttribute(TaskcontrollerNS.ID.getValue(), filename, omNs); 
			 files.addChild(file);
			 strContent += "Process log:" + filename + "\n\n" + partialLogs.get(filename) + "\n\n";
		 }
		 fileContent.setText(strContent);
		 logs.addChild(files);
		 logs.addChild(fileContent);
		 message.addChild(logs);
		 
		 return message;
	 }
	 
	 public static OMElement getCheckAndStartUpDBIfNecessaryMessage(String gridname) {
//		 OMNamespace omNs = fac.createOMNamespace("http://grid.imcs.com/topology", prefix);
		 OMElement method = fac.createOMElement(TaskcontrollerNS.CHECKANDSTARTUPDBIFNECESSARY.getValue(), omNs);
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 method.addChild(gridnameElem);
		 return method;
	 }
	 
	 public static OMElement getMessageAddStatisticsAlebra(String jcl,String step, String pid, String idExecution,
				double cpu_download,double elapsed_download, double cpu_upload, double elapsed_upload, int num_files_in,
				long size_files_in, int num_cache_files_in, long size_cache_files_in, int num_files_out, long size_files_out, 
				int redundance_code, ArrayList<String> files_sizes, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDSTATISTICSALEBRA.getValue(), omNs);
		 
		 OMElement jclOm = fac.createOMElement(TaskcontrollerNS.JCL.getValue(), omNs);
		 jclOm.addChild(fac.createOMText(jclOm, jcl));
		 OMElement stepOm = fac.createOMElement(TaskcontrollerNS.STEP.getValue(), omNs);
		 stepOm.addChild(fac.createOMText(stepOm, step));
		 OMElement pidOm = fac.createOMElement(TaskcontrollerNS.PID.getValue(), omNs);
		 pidOm.addChild(fac.createOMText(pidOm, pid));
		 OMElement idExecOm = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		 idExecOm.addChild(fac.createOMText(idExecOm, idExecution));
		 OMElement cpuDownOm = fac.createOMElement(TaskcontrollerNS.CPUDOWNLOAD.getValue(), omNs);
		 cpuDownOm.addChild(fac.createOMText(cpuDownOm, String.valueOf(cpu_download)));
		 OMElement elapDownOm = fac.createOMElement(TaskcontrollerNS.ELAPSEDDOWNLOAD.getValue(), omNs);
		 elapDownOm.addChild(fac.createOMText(elapDownOm, String.valueOf(elapsed_download)));
		 OMElement cpuUpOm = fac.createOMElement(TaskcontrollerNS.CPUUPLOAD.getValue(), omNs);
		 cpuUpOm.addChild(fac.createOMText(cpuUpOm, String.valueOf(cpu_upload)));
		 OMElement elapUpOm = fac.createOMElement(TaskcontrollerNS.ELAPSEDUPLOAD.getValue(), omNs);
		 elapUpOm.addChild(fac.createOMText(elapUpOm, String.valueOf(elapsed_upload)));
		 OMElement numFilesInOm = fac.createOMElement(TaskcontrollerNS.NUMFILESIN.getValue(), omNs);
		 numFilesInOm.addChild(fac.createOMText(numFilesInOm, String.valueOf(num_files_in)));
		 OMElement sizeFilesInOm = fac.createOMElement(TaskcontrollerNS.SIZEFILESIN.getValue(), omNs);
		 sizeFilesInOm.addChild(fac.createOMText(sizeFilesInOm, String.valueOf(size_files_in)));
		 OMElement numCacheFilesInOm = fac.createOMElement(TaskcontrollerNS.NUMCACHEFILESIN.getValue(), omNs);
		 numCacheFilesInOm.addChild(fac.createOMText(numCacheFilesInOm, String.valueOf(num_cache_files_in)));
		 OMElement sizeCacheFilesInOm = fac.createOMElement(TaskcontrollerNS.SIZECACHEFILESIN.getValue(), omNs);
		 sizeCacheFilesInOm.addChild(fac.createOMText(sizeCacheFilesInOm, String.valueOf(size_cache_files_in)));
		 OMElement numFilesOutOm = fac.createOMElement(TaskcontrollerNS.NUMFILESOUT.getValue(), omNs);
		 numFilesOutOm.addChild(fac.createOMText(numFilesOutOm, String.valueOf(num_files_out)));
		 OMElement sizeFilesOutOm = fac.createOMElement(TaskcontrollerNS.SIZEFILESOUT.getValue(), omNs);
		 sizeFilesOutOm.addChild(fac.createOMText(sizeFilesOutOm, String.valueOf(size_files_out)));
		 OMElement codeOm = fac.createOMElement(TaskcontrollerNS.REDUNDANCECODE.getValue(), omNs);
		 codeOm.addChild(fac.createOMText(codeOm, String.valueOf(redundance_code)));
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
			
		 message.addChild(jclOm);
		 message.addChild(stepOm);
		 message.addChild(pidOm);
		 message.addChild(idExecOm);
		 message.addChild(cpuDownOm);
		 message.addChild(elapDownOm);
		 message.addChild(cpuUpOm);
		 message.addChild(elapUpOm);
		 message.addChild(numFilesInOm);
		 message.addChild(sizeFilesInOm);
		 message.addChild(numCacheFilesInOm);
		 message.addChild(sizeCacheFilesInOm);
		 message.addChild(numFilesOutOm);
		 message.addChild(sizeFilesOutOm);
		 message.addChild(codeOm);
		 message.addChild(gridnameElem);
		 
		 if (!files_sizes.isEmpty()) {
			 OMElement outputFiles = fac.createOMElement(TaskcontrollerNS.FILES.getValue(), omNs);
			 OMElement file = null;
			 for (String file_size : files_sizes) {
				 file = fac.createOMElement(TaskcontrollerNS.FILE.getValue(), omNs);
				 file.addAttribute(fac.createOMAttribute(TaskcontrollerNS.NAME.getValue(), omNs, file_size.split("-")[0]));
				 file.addAttribute(fac.createOMAttribute(TaskcontrollerNS.SIZE.getValue(), omNs, file_size.split("-")[1]));
				 outputFiles.addChild(file);
			 }
			 message.addChild(outputFiles);
		 }
		 return message;
	 }
	 
	 public static OMElement getMessageAddServiceExecution(String gridpid, String jcl,String step, String idNode, String idService, 
				String result, String rc, String rcDescription, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDSERVICEEXECUTION.getValue(), omNs);
		 
		 OMElement pidOm = fac.createOMElement(TaskcontrollerNS.PID.getValue(), omNs);
		 pidOm.addChild(fac.createOMText(pidOm, gridpid));
		 OMElement jclOm = fac.createOMElement(TaskcontrollerNS.JCL.getValue(), omNs);
		 jclOm.addChild(fac.createOMText(jclOm, jcl));
		 OMElement stepOm = fac.createOMElement(TaskcontrollerNS.STEP.getValue(), omNs);
		 stepOm.addChild(fac.createOMText(stepOm, step));
		 OMElement nodeOm = fac.createOMElement(TaskcontrollerNS.NODE.getValue(), omNs);
		 nodeOm.addChild(fac.createOMText(nodeOm, idNode));
		 OMElement servOm = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
		 servOm.addChild(fac.createOMText(servOm, idService));
		 OMElement resultOm = fac.createOMElement(TaskcontrollerNS.RESULT.getValue(), omNs);
		 resultOm.addChild(fac.createOMText(resultOm, result));
		 OMElement rcOm = fac.createOMElement(TaskcontrollerNS.RC.getValue(), omNs);
		 rcOm.addChild(fac.createOMText(resultOm, rc));
		 OMElement rcDescOm = fac.createOMElement(TaskcontrollerNS.DESC.getValue(), omNs);
		 rcDescOm.addChild(fac.createOMText(rcDescOm, rcDescription));
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 
		 message.addChild(pidOm);
		 message.addChild(jclOm);
		 message.addChild(stepOm);
		 message.addChild(nodeOm);
		 message.addChild(servOm);
		 message.addChild(resultOm);
		 message.addChild(rcOm);
		 message.addChild(rcDescOm);
		 message.addChild(gridnameElem);
		 
		 return message;
	 }
	 
	 public static OMElement getMessageAddResultParallel(String jcl,String step, String nameFile1, String nameFile2, String result, String gridname,
			 											 String fileCompareType,String rcActual,String rcDescActual) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDRESULTPARALLEL.getValue(), omNs);
		 
		 OMElement jclOm = fac.createOMElement(TaskcontrollerNS.JCL.getValue(), omNs);
		 jclOm.addChild(fac.createOMText(jclOm, jcl));
		 OMElement stepOm = fac.createOMElement(TaskcontrollerNS.STEP.getValue(), omNs);
		 stepOm.addChild(fac.createOMText(stepOm, step));
		 OMElement nameOm = fac.createOMElement(TaskcontrollerNS.NAME.getValue(), omNs);
		 nameOm.addAttribute("name1", nameFile1, omNs);
		 nameOm.addAttribute("name2", nameFile2, omNs);
		 OMElement resultOm = fac.createOMElement(TaskcontrollerNS.RESULT.getValue(), omNs);
		 resultOm.addChild(fac.createOMText(resultOm, result));
		 OMElement gridnameElemOM = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElemOM.addChild(fac.createOMText(gridnameElemOM, gridname));
		 OMElement fileCompareTypeOM =fac.createOMElement(TaskcontrollerNS.FCT.getValue(), omNs);
		 fileCompareTypeOM.addChild(fac.createOMText(fileCompareTypeOM, fileCompareType));
		 OMElement rcActualOM = fac.createOMElement(TaskcontrollerNS.RCACTUAL.getValue(), omNs);
		 rcActualOM.addChild(fac.createOMText(rcActualOM, rcActual));
		 OMElement rcDescActualOM = fac.createOMElement(TaskcontrollerNS.RCDESACTUAL.getValue(), omNs);
		 rcDescActualOM.addChild(fac.createOMText(rcDescActualOM, rcDescActual));
		 
		 message.addChild(jclOm);
		 message.addChild(stepOm);
		 message.addChild(nameOm);
		 message.addChild(resultOm);
		 message.addChild(gridnameElemOM);
		 message.addChild(fileCompareTypeOM);
		 message.addChild(rcActualOM);
		 message.addChild(rcDescActualOM);
		 return message;
	 }
	 
	 public static OMElement getMessageEndExecution(String idExecution,String pidGrid, String endDate, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ENDEXECUTION.getValue(), omNs);
		 
		 OMElement idExecOm = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		 idExecOm.addChild(fac.createOMText(idExecOm, idExecution));
		 OMElement pidOm = fac.createOMElement(TaskcontrollerNS.PID.getValue(), omNs);
		 pidOm.addChild(fac.createOMText(pidOm, pidGrid));
		 OMElement dateOm = fac.createOMElement(TaskcontrollerNS.DATE.getValue(), omNs);
		 dateOm.addChild(fac.createOMText(dateOm, endDate));
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 
		 message.addChild(idExecOm);
		 message.addChild(pidOm);
		 message.addChild(dateOm);
		 message.addChild(gridnameElem);
		 
		 return message;
	 }
	 
	 public static OMElement getMessageAddGridAvailability(String id, String node, Integer availableNodes, Long initTime, String service, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDGRIDAVAILABILITY.getValue(), omNs);
		 OMElement idOm = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		 idOm.addChild(fac.createOMText(idOm, id));
		 OMElement nodeOm = fac.createOMElement(TaskcontrollerNS.NODE.getValue(), omNs);
		 nodeOm.addChild(fac.createOMText(nodeOm, node));
		 OMElement serviceOm = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
		 serviceOm.addChild(fac.createOMText(serviceOm, service));
		 //OMElement availableNodesOm = fac.createOMElement(TaskcontrollerNS.AVAILABLENODES.getValue(), omNs);
		 //availableNodesOm.addChild(fac.createOMText(availableNodesOm, availableNodes.toString()));
		 OMElement initTimeOm = fac.createOMElement(TaskcontrollerNS.INITIME.getValue(), omNs);
		 initTimeOm.addChild(fac.createOMText(initTimeOm, initTime.toString()));		 
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 
		 message.addChild(idOm);
		 message.addChild(nodeOm);
		 message.addChild(serviceOm);
		 message.addChild(initTimeOm);
		 message.addChild(gridnameElem);
		 
		 return message;
	 }
	 
	 public static OMElement getMessageAddExecution(String idExec, String exec, String idNode, String idSession,
				String idService, String pidGrid, String description, String initDate, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDEXECUTION.getValue(), omNs);
		 
		 OMElement idExecOm = fac.createOMElement(TaskcontrollerNS.ID.getValue(), omNs);
		 idExecOm.addChild(fac.createOMText(idExecOm, idExec));
		 OMElement execOm = fac.createOMElement(TaskcontrollerNS.EXEC.getValue(), omNs);
		 execOm.addChild(fac.createOMText(execOm, exec));
		 OMElement nodeOm = fac.createOMElement(TaskcontrollerNS.NODE.getValue(), omNs);
		 nodeOm.addChild(fac.createOMText(nodeOm, idNode));
		 OMElement sessionOm = fac.createOMElement(TaskcontrollerNS.SESSION.getValue(), omNs);
		 sessionOm.addChild(fac.createOMText(sessionOm, idSession));
		 OMElement servOm = fac.createOMElement(TaskcontrollerNS.SERVICE.getValue(), omNs);
		 servOm.addChild(fac.createOMText(servOm, idService));
		 OMElement pidOm = fac.createOMElement(TaskcontrollerNS.PID.getValue(), omNs);
		 pidOm.addChild(fac.createOMText(pidOm, pidGrid));
		 OMElement descOm = fac.createOMElement(TaskcontrollerNS.DESC.getValue(), omNs);
		 descOm.addChild(fac.createOMText(descOm, description));
		 OMElement dateOm = fac.createOMElement(TaskcontrollerNS.DATE.getValue(), omNs);
		 dateOm.addChild(fac.createOMText(dateOm, initDate));
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 
		 message.addChild(idExecOm);
		 message.addChild(execOm);
		 message.addChild(nodeOm);
		 message.addChild(sessionOm);
		 message.addChild(servOm);
		 message.addChild(pidOm);
		 message.addChild(descOm);
		 message.addChild(dateOm);
		 message.addChild(gridnameElem);
		 
		 return message;
	 }
	 
	 public static OMElement getMessageAddServices(String[] services, String[] description, String gridname) {
		 OMElement message = fac.createOMElement(TaskcontrollerNS.ADDSERVICES.getValue(), omNs);
		 
		 OMElement servicesOm = null;
		 for (int i=0,iHasta=services.length; i<iHasta; i++) {
			 servicesOm = fac.createOMElement(TaskcontrollerNS.SERVICES.getValue(), omNs);
			 servicesOm.addAttribute(TaskcontrollerNS.SERVICE.getValue(), services[i], omNs);
			 servicesOm.addAttribute(TaskcontrollerNS.DESC.getValue(), description[i], omNs);
			 message.addChild(servicesOm);
		 }
		 OMElement gridnameElem = fac.createOMElement(TaskcontrollerNS.GRIDNAME.getValue(), omNs);
		 gridnameElem.addChild(fac.createOMText(gridnameElem, gridname));
		 message.addChild(gridnameElem);
		 return message;
	 }
}
