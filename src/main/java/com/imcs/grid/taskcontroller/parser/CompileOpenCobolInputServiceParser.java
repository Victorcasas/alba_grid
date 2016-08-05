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
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;
import com.imcs.grid.util.FileInfo;

public class CompileOpenCobolInputServiceParser extends DefaultInputServiceParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(CompileOpenCobolInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init Parse request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] param = parserCompileCobolMessage(ome);
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

    private DefaultParam[] parserCompileCobolMessage(OMElement ome) throws TaskcontrollerParserException {
    	    	
    	/* <mvsinfo jcl="value" step="value" /> */
    	OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
    	if (mvsInfo == null)
    		throw new TaskcontrollerParserException("Error, mvsinfo tag is null or empty.", ErrorType.ERROR);
		
    	String jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
		String step = mvsInfo.getAttributeValue(new QName("", "step"));
		
    	/* <pgm id="value" timestamp="value" /> */
		OMElement pgm = ome.getFirstChildWithName(new QName("", "pgm"));
		if (pgm == null) 
			throw new TaskcontrollerParserException("Error, pgm tag is null or empty.", ErrorType.ERROR);
		
		String id = pgm.getAttributeValue(new QName("", "id"));
		String timestamp = pgm.getAttributeValue(new QName("", "timestamp"));
		
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
									 new DefaultParam("timestamp", timestamp), 
									 new DefaultParam("files", inputsFileInfo) };		
		return parameter;
    }
    
    public OMElement responseParse(Response input) throws TaskControllerException {
		
		OMFactory factory = OMAbstractFactory.getOMFactory();
//		OMNamespace name = factory.createOMNamespace("http://grid.imcs.com/taskcontroller", "");
		OMNamespace name = factory.createOMNamespace("", "");
		
		OMElement result = factory.createOMElement("result", name);
		
		OMElement compile = factory.createOMElement("compile-cobol", name);		
		String message = (String)input.get("responseCompileCobol");
		compile.setText(message);
		
		OMElement rc = factory.createOMElement("rc", name);
		String gridrc = (String)input.get("gridrc");
		String exitStatusCode = (String)input.get("exitStatusCode");
		rc.addAttribute("value", gridrc, name);
		rc.addAttribute("finish", exitStatusCode, name);
		
		result.addChild(rc);
		result.addChild(compile);
		
		return result;
	}
}