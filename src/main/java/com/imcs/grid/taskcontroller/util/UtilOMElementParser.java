package com.imcs.grid.taskcontroller.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.parser.TaskcontrollerParserException;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class UtilOMElementParser {

	/**
	 * Parse labels and files from inputs, outputs or mod
	 * @param ome
	 * @return
	 * 		List of FileInfo
	 * 		If the list is null or size list is 0 then throws exception 
	 * @throws TaskControllerException
	 */
	public static List<FileInfo> parserFiles(String type, OMElement ome, boolean validateSize) 
	throws TaskcontrollerParserException {
		
		List<FileInfo> lFileInfo = new ArrayList<FileInfo>();
		Iterator<?> labels = ome.getChildElements();
		while (labels.hasNext()) {
			
			OMElement label = (OMElement) labels.next();
			if (label == null) 
				throw new TaskcontrollerParserException("Error, label tag is null.", ErrorType.ERROR);
			
			String labelName = label.getAttributeValue(new QName("", "name"));
			if (UtilString.isNullOrEmpty(labelName)) 
				throw new TaskcontrollerParserException("Error, label is null or empty.", ErrorType.ERROR);
			
			String labelType = "";
			if (labelName.startsWith("src:")) {
				labelType = "src";
				labelName = labelName.substring("src:".length());
			}
			else if (labelName.startsWith("copy:")) {
				labelType = "copy";
				labelName = labelName.substring("copy:".length());
			}
			
			Iterator<?> files = label.getChildElements();			
			while (files.hasNext()) {						
				
				OMElement file = (OMElement)files.next();
				if (file == null) 
					throw new TaskcontrollerParserException("Error, file is null.", ErrorType.ERROR);
				
				String fileName = file.getAttributeValue(new QName("", "name"));
				if (UtilString.isNullOrEmpty(fileName)) 
					throw new TaskcontrollerParserException("Error, name of file is null or empty for label " + labelName, ErrorType.ERROR);
				
				String recordLen = file.getAttributeValue(new QName("", "length"));
				if (UtilString.isNullOrEmpty(recordLen)) 
					throw new TaskcontrollerParserException("Error, length is null or empty " + fileName, ErrorType.ERROR);
				
				String fileDisp = file.getAttributeValue(new QName("", "disp"));
				
				String records = file.getAttributeValue(new QName("", "records"));
				
				String size = file.getAttributeValue(new QName("", "size"));
				
				String dcb = file.getAttributeValue(new QName("", "dcb"));
				
				FileInfo fi = new FileInfo(fileName, labelName, recordLen, fileDisp, records, labelType, size, dcb);
								
				lFileInfo.add(fi);
			}
		}
		if ((lFileInfo.size()==0) && (validateSize))
			throw new TaskcontrollerParserException("Not files for " + type, ErrorType.ERROR);
		
		return lFileInfo;
	}
}
