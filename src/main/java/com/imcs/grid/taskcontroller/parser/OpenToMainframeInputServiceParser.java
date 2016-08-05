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
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;
import com.imcs.grid.util.FileInfo;

public class OpenToMainframeInputServiceParser extends DefaultInputServiceParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(OpenToMainframeInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] param = openToMainframe(ome);
				for (int i = 0; i < param.length; i++) 
					req.put(param[i].getKey(),param[i].getValue());
			} else {
				logger.info("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request  ");
		logger.debug("Request " + req);	
		return req;
	}

    private DefaultParam[] openToMainframe(OMElement ome) throws TaskcontrollerParserException {
    	
    	OMElement noAllocate = ome.getFirstChildWithName(new QName("","noallocate"));
    	String enabled = null;
    	if (noAllocate != null) {
    		logger.debug("no allocate tag found");
    		enabled = noAllocate.getAttributeValue(new QName("","enabled"));
    		if (enabled == null)
    			enabled = noAllocate.getAttributeValue(new QName("","enabled"));
    		if (enabled != null && enabled.equals("true")) {
    			logger.info("no allocate parse");
    			DefaultParam[] dp =  {new DefaultParam("noallocate.enabled", enabled)};
    			return dp;
    		}
    		else if (enabled == null)
    			logger.warn("noallocate tag found and enable not found");
    	}
    	
    	/* <mvsinfo jcl="value" step="value" /> */
		OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
		if (mvsInfo == null)
			throw new TaskcontrollerParserException("Error, mvsinfo tag is null or empty.", ErrorType.ERROR);
			
		String jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
		String step = mvsInfo.getAttributeValue(new QName("", "step"));
		
		/* <pgm id="value" /> */
		OMElement pgm = ome.getFirstChildWithName(new QName("", "pgm"));
		if (pgm == null) 
			throw new TaskcontrollerParserException("Error, pgm tag is null or empty.", ErrorType.ERROR);
		
		String id = pgm.getAttributeValue(new QName("", "id"));
		
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
//		OMElement inputs = ome.getFirstChildWithName(new QName("", "inputs"));
//		if (inputs == null)
//			throw new TaskControllerException("Error, inputs tag is null or empty.", ErrorType.ERROR);
//		
//		List<FileInfo> inputsfiles = UtilOMElementParser.parserFiles("inputs", inputs);
//		FileInfo[] inputsFileInfo = (FileInfo[])inputsfiles.toArray(new FileInfo[0]);		
			
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
		OMElement outputs = ome.getFirstChildWithName(new QName("", "outputs"));
		if (outputs == null)
			throw new TaskcontrollerParserException("Error, outputs tag is null or empty.", ErrorType.ERROR);
		
		List<FileInfo> outputsFiles = UtilOMElementParser.parserFiles("outputs", outputs, true);
		FileInfo[] outputsFileInfo = (FileInfo[])outputsFiles.toArray(new FileInfo[0]);
		
		DefaultParam[] parameter = { new DefaultParam("jcl", jcl), 
									 new DefaultParam("step", step),
		 							 new DefaultParam("id", id),
		 							 new DefaultParam("files", outputsFileInfo)};
    	return parameter;
    }

	public OMElement responseParse(Response input) throws TaskControllerException {
		return null;
	}
}