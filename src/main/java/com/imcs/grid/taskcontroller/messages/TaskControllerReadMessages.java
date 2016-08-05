package com.imcs.grid.taskcontroller.messages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.taskcontroller.ns.TaskcontrollerNS;

public class TaskControllerReadMessages {
	
//	private static String brokerNS = TaskcontrollerNS.BROKERNS.getValue();
	private static String ns = "";
	
	public static String getRootLocation(OMElement message)	{
		return message.getFirstChildWithName(new QName(TaskcontrollerNS.ROOTLOCATION.getValue())).getText();
	}
	
	public static String getPidAttribute(OMElement message)	{
		return message.getAttributeValue(new QName("",TaskcontrollerNS.PID.getValue()));
	}
		
	public static String getUniqueSessionId(OMElement message) {
		return message.getAttributeValue(new QName("", TaskcontrollerNS.UNIQUESESSIONID.getValue()));
	}
	
	public static String getOperationAttribute(OMElement message) {
		return message.getAttributeValue(new QName("", TaskcontrollerNS.OPERATION.getValue()));
	}
	
	public static String getDocumentAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.DOCUMENT.getValue()));
	}
	 
	public static String getNewRootLocationAttribute(OMElement message) {
		return message.getAttributeValue(new QName("", TaskcontrollerNS.NEWROOTLOCATION.getValue()));
	}
	 
	public static String getGridNameAttribute(OMElement message) {
		return message.getAttributeValue(new QName("", TaskcontrollerNS.GRIDNAME.getValue()));
	}
	 
	public static String getFileAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.FILE.getValue()));
	}
	 
	public static String getPortAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.PORT.getValue()));
	}
	 
	public static String getNameAttribute(OMElement message) {
		return message.getAttributeValue(new QName(ns, TaskcontrollerNS.NAME.getValue()));
	}
	 
	public static String getCmdAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.CMD.getValue()));
	} 
	 
	public static String getParamsAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.PARAMS.getValue()));
	}
	 
	public static String getTimeAttribute(OMElement message) {
		return message.getAttributeValue(new QName(TaskcontrollerNS.TIME.getValue()));
	}
	 
	public static String getFileText(OMElement message) {
		OMElement file = message.getFirstChildWithName(new QName("", TaskcontrollerNS.FILE.getValue()));
		return file.getText();
	}
	 
	public static String getFoldersText(OMElement message) {
		OMElement folders = message.getFirstChildWithName(new QName("", TaskcontrollerNS.FOLDERS.getValue()));
		return folders.getText();
	}
	 
	public static String getExtensionsText(OMElement message) {
		OMElement extensions = message.getFirstChildWithName(new QName("", TaskcontrollerNS.EXTENSIONS.getValue()));
		return extensions.getText();
	}
	 
	public static String getLocationText(OMElement message) {
		OMElement location = message.getFirstChildWithName(new QName("", TaskcontrollerNS.LOCATION.getValue()));
		return location.getText();
	}
	 
	public static String getTitleText(OMElement message) {
		OMElement title = message.getFirstChildWithName(new QName("", TaskcontrollerNS.TITLE.getValue()));
		return title.getText();
	}
	 
	public static String getGridNameText(OMElement message) {
		OMElement gridname = message.getFirstChildWithName(new QName("", TaskcontrollerNS.GRIDNAME.getValue()));
		if (gridname != null)
			return gridname.getText();
		else
			return "";
	}
	 
	public static HashMap<String, ArrayList<String>> getFoldersAndExtensions(OMElement message) {
		HashMap<String, ArrayList<String>> result = new HashMap<String, ArrayList<String>>();
		
		OMElement omCache = message.getFirstChildWithName(new QName(TaskcontrollerNS.CACHE.getValue()));
		Iterator<?> itFolder = omCache.getChildrenWithName(new QName("",TaskcontrollerNS.FOLDER.getValue()));
		while (itFolder.hasNext()) {
			OMElement omFolder = (OMElement)itFolder.next();
			OMElement omName = omFolder.getFirstChildWithName(new QName(TaskcontrollerNS.NAME.getValue()));
			String folderName = omName.getText();
			
			ArrayList<String> extensions = new ArrayList<String>();
			OMElement omExtensions = omFolder.getFirstChildWithName(new QName(TaskcontrollerNS.EXTENSIONS.getValue()));
			Iterator<?> itExtension = omExtensions.getChildrenWithName(new QName(TaskcontrollerNS.EXTENSION.getValue()));
			while (itExtension.hasNext()) {
				OMElement omExtension = (OMElement)itExtension.next();
				extensions.add(omExtension.getText());
			}
			result.put(folderName, extensions);
		}
		return result;	
	}
	 
	public static List<String> getListChildrens(OMElement input, String node) {
           List <String> list = new ArrayList<String>();
           Iterator <?> itNodes = input.getChildrenWithName(new QName ("", node));
           OMElement nodeChild;
           while (itNodes.hasNext()) {
                 nodeChild = (OMElement) itNodes.next();
                 list.add(nodeChild.getText());
           }
           return list;
     }

	public static ArrayList<String> getNodesText(OMElement message) {
          ArrayList<String> res = new ArrayList<String>();
          Iterator<?> itNodes = message.getChildrenWithName(new QName(TaskcontrollerNS.NODE.getValue()));
          while (itNodes.hasNext())
                res.add(((OMElement) itNodes.next()).getText());
          
          return res;
    }
    
	public static ArrayList<String> getDatesText(OMElement message) {
          ArrayList<String> res = new ArrayList<String>();
          Iterator<?> itDates = message.getChildrenWithName(new QName(TaskcontrollerNS.DATE.getValue()));
          while (itDates.hasNext())
        	  res.add(((OMElement) itDates.next()).getText());
          
          return res;
	}

	public static boolean getFinishJobResponse(OMElement message) {
		boolean send = false;
		String resp = message.getLocalName();
		if(resp.equals("unSetRunningResponse"))
			send = true;
		 return send;
	}
	
	public static String getTotalTimeText(OMElement message) {
		String namespace = "";
		if (message.getNamespace() != null)
			namespace = message.getNamespace().getNamespaceURI();
		OMElement totalTime = message.getFirstChildWithName(new QName(namespace, TaskcontrollerNS.TOTALTIME.getValue()));
		if (totalTime == null)
			return "";
		else
			return totalTime.getText();
	}
	
	public static String getServiceTimeText(OMElement message) {
		String namespace = "";
		if (message.getNamespace() != null)
			namespace = message.getNamespace().getNamespaceURI();
		OMElement serviceTime = message.getFirstChildWithName(new QName(namespace, TaskcontrollerNS.SERVICETIME.getValue()));
		if (serviceTime == null)
			return "";
		else
			return serviceTime.getText();
	}
}
