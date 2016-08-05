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
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class AhlsortInputServiceParser extends DefaultInputServiceParser {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(AhlsortInputServiceParser.class);
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("INIT parser request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null;
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome  = (OMElement)obj;
				DefaultParam[] parameter = ahlsort(ome);
				for (int i = 0; i < parameter.length; i++) {
					logger.debug(parameter[i].getKey() + "" + parameter[i].getValue());
					req.put(parameter[i].getKey(), parameter[i].getValue());
				}
			} else {
				logger.error("Detected not default param " + obj.getClass());
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request");
		logger.debug("Request " + req);
		return req;
	}
	
	private DefaultParam[] ahlsort(OMElement ome) throws TaskcontrollerParserException {
		String mode = ome.getAttributeValue(new QName("","mode"));
		if ((mode == null) || (!mode.equalsIgnoreCase("parallel")))
			mode = "normal";
		
		/* <mvsinfo jcl="value" step="value" /> */
		OMElement mvsInfo = ome.getFirstChildWithName(new QName("","mvsinfo"));
		if (mvsInfo == null)
			throw new TaskcontrollerParserException("Error, mvsinfo tag is null or empty.", ErrorType.ERROR);
		String jcl = mvsInfo.getAttributeValue(new QName("","jcl"));
		String step = mvsInfo.getAttributeValue(new QName("","step"));
		
		/* <sysin>"OPTION COPY"</sysin> */
		OMElement sysin = ome.getFirstChildWithName(new QName("","sysin"));
		if (sysin == null)
			throw new TaskcontrollerParserException("Error, sysin in null.", ErrorType.ERROR);
		
		String text_sysin = sysin.getText();
		if (UtilString.isNullOrEmpty(text_sysin))
			throw new TaskcontrollerParserException("Error, sysin is null or empty.", ErrorType.ERROR);
		
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
		
		List<FileInfo> inputsFiles = UtilOMElementParser.parserFiles("inputs", inputs, true);
		FileInfo[] inputsFileInfo = (FileInfo[])inputsFiles.toArray(new FileInfo[0]);
		
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
		
		List<FileInfo> outputsFiles = UtilOMElementParser.parserFiles("outputs", outputs, true); 
		FileInfo[] outputsFileInfo = (FileInfo[])outputsFiles.toArray(new FileInfo[0]);
		
		DefaultParam[] parameter = { new DefaultParam("mode", mode),
									 new DefaultParam("jcl", jcl),
									 new DefaultParam("step", step),
									 new DefaultParam("sysin", text_sysin),
									 new DefaultParam("files", inputsFileInfo),
									 new DefaultParam("filesOutput", outputsFileInfo) };
		return parameter;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement ome = factory.createOMElement("gridjob-response", name);
		
		String mode = (String)input.get("mode");
		if (mode.equalsIgnoreCase("parallel")) {
			ome.addAttribute("recordToMds","true,",name);
			
			OMElement resultCompare = factory.createOMElement("result", name);
			String result = (String)input.get("result");
			resultCompare.setText(result);
			ome.addChild(resultCompare);
			
			OMElement gridRc = factory.createOMElement("gridrc", name);
			String rc = (String)input.get("gridrc");
			gridRc.setText(rc);
			ome.addChild(gridRc);
		}
		return ome;
	}
}
