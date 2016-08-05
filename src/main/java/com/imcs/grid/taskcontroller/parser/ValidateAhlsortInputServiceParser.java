package com.imcs.grid.taskcontroller.parser;

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.DefaultParam;
import com.imcs.grid.taskcontroller.util.UtilOMElementParser;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class ValidateAhlsortInputServiceParser extends DefaultInputServiceParser {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(ValidateAhlsortInputServiceParser.class);	
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("INIT parser request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] parameter = sort(ome);
				for (int i = 0; i < parameter.length; i++) {
					logger.debug(parameter[i].getKey() + " " + parameter[i].getValue());
					req.put(parameter[i].getKey(),parameter[i].getValue());
				}
			} else {
				logger.error("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}		
		logger.info("Created request");
		logger.debug("Request " + req);
		return req;
	}
	
	public OMElement responseParse(Response response) throws TaskControllerException {
		return null;
	}

	private DefaultParam[] sort(OMElement ome) throws TaskcontrollerParserException {
		OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
		String jcl = null;
		String step = null;	
		if (mvsInfo != null) {
			jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
			step = mvsInfo.getAttributeValue(new QName("", "step"));
			logger.info("JCL:: " + jcl);
			logger.info("Step:: " + step);		
		} else
			logger.warn("MVSINFO is empty");
		DefaultParam jclParam = new DefaultParam("jcl", jcl);
		DefaultParam stepParam = new DefaultParam("step", step);
		
		OMElement sysin = ome.getFirstChildWithName(new QName("", "sysin"));
		if (sysin == null) 
			throw new TaskcontrollerParserException("Error, Sysin is null.", ErrorType.ERROR);
		String text_sysin = sysin.getText();
		if (UtilString.isNullOrEmpty(text_sysin)) 
			throw new TaskcontrollerParserException("Error, Sysin is null or empty.", ErrorType.ERROR);
		
		DefaultParam sysinParam = new DefaultParam("sysin", text_sysin);
		logger.info("Sysin to validate :: " + text_sysin);
		
		/* Example:
		 * <inputs>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * </inputs> 
		 * */
		OMElement inputs = ome.getFirstChildWithName(new QName("","inputs"));
		if (inputs == null)
			throw new TaskcontrollerParserException("Error, inputs tag is null or empty.", ErrorType.ERROR);
		
		FileInfo[] inputsFileInfo = null;
		try {
			List<FileInfo> inputsFiles = UtilOMElementParser.parserFiles("inputs", inputs, true);
			inputsFileInfo = (FileInfo[])inputsFiles.toArray(new FileInfo[0]);	
		} catch (TaskcontrollerParserException tcpe) {
			logger.warn(tcpe.getMessage());
			inputsFileInfo = new FileInfo[0];
		}
		DefaultParam inputFilesParam = new DefaultParam("files", inputsFileInfo);
		
		/* Example:
		 * <outputs>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * </outputs> 
		 * */
		OMElement outputs = ome.getFirstChildWithName(new QName("","outputs"));
		if (outputs == null)
			throw new TaskcontrollerParserException("Error, outputs tag is null or empty.", ErrorType.ERROR);
		
		FileInfo[] outputsFileInfo = null;
		try {
			List<FileInfo> outputsFiles = UtilOMElementParser.parserFiles("outputs", outputs, true); 
			outputsFileInfo = (FileInfo[])outputsFiles.toArray(new FileInfo[0]);
		} catch (TaskcontrollerParserException tcpe) {
			logger.warn(tcpe.getMessage());
			outputsFileInfo = new FileInfo[0];
		}
		DefaultParam outputFilesParam = new DefaultParam("filesOutput", outputsFileInfo);
		
		
		DefaultParam[] parameter = {jclParam, stepParam, sysinParam, inputFilesParam, outputFilesParam};
    	return parameter;
	}
}