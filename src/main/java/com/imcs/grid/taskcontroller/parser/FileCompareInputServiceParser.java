package com.imcs.grid.taskcontroller.parser;

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.DefaultParam;
import com.imcs.grid.taskcontroller.util.UtilOMElementParser;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;
import com.imcs.grid.util.FileInfo;

public class FileCompareInputServiceParser extends DefaultInputServiceParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(FileCompareInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init parser - Request :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] parameter = fileCompare(ome);
				for (int i = 0; i < parameter.length; i++)
					req.put(parameter[i].getKey(),parameter[i].getValue());
			} else {
				logger.info("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}		
		logger.info("Created request");
		logger.debug("Request " + req);
		return req;
	}
	
	public OMElement responseParse(Response output) throws TaskControllerException {
		OMFactory factory = OMAbstractFactory.getOMFactory();
//		OMNamespace name = factory.createOMNamespace("http://grid.imcs.com/taskcontroller", "");
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement ome = factory.createOMElement("gridjob-response", name);
		
		if (output.containsKey("gridrc")) {
			OMElement omeGridRc = factory.createOMElement("gridrc", name);
			omeGridRc.setText((String)output.get("gridrc"));
			ome.addChild(omeGridRc);
		}
		OMElement omeFileCompare = factory.createOMElement("result", name);
	    omeFileCompare.setText((String)output.get("result"));
	    ome.addChild(omeFileCompare);
		return ome;
	}

    private DefaultParam[] fileCompare(OMElement ome) throws TaskcontrollerParserException {
    	
    	String mode = ome.getAttributeValue(new QName("","mode"));
		if ((mode == null) || (!mode.equalsIgnoreCase("parallel"))) 
    		mode = "normal";     		

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
		
		/* <sysin>"OPTION COPY"</sysin> */
		String text_sysin = "";
		OMElement sysin = ome.getFirstChildWithName(new QName("", "sysin"));
		if (sysin == null)  
			text_sysin = "";
		else 
			text_sysin = sysin.getText();
	
		if (UtilString.isNullOrEmpty(text_sysin)) 
			text_sysin = "";
		
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
		OMElement inputs = ome.getFirstChildWithName(new QName("", "inputs"));	
		if (inputs == null)
			throw new TaskcontrollerParserException("Error, inputs tag is null or empty.", ErrorType.ERROR);
		
		List<FileInfo> inputsfiles = UtilOMElementParser.parserFiles("inputs", inputs, true);
		FileInfo[] inputsFileInfo = (FileInfo[])inputsfiles.toArray(new FileInfo[0]);		
		
		DefaultParam[] parameter = { new DefaultParam("jcl", jcl), 
				 					 new DefaultParam("step", step), 
				 					 new DefaultParam("id", id), 
				 					 new DefaultParam("files", inputsFileInfo), 
				 					 new DefaultParam("sysin", text_sysin) };
    	return parameter;
    }
}