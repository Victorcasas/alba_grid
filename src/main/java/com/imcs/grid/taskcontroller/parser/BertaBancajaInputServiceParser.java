package com.imcs.grid.taskcontroller.parser;

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
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class BertaBancajaInputServiceParser extends DefaultInputServiceParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(BertaBancajaInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] parameter = parserExecuteBertaBancajaMessage(ome);
				Object value = null;
				for (int i = 0; i < parameter.length; i++) {
					value = parameter[i].getValue();
					if (value != null) 
						req.put(parameter[i].getKey(), value);
				}
			} else {
				logger.info("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request");
		logger.debug("Request " + req);
		return req;
	}

	public DefaultParam[] parserExecuteBertaBancajaMessage(OMElement ome) throws TaskcontrollerParserException {
		
		String mode = ome.getAttributeValue(new QName("","mode"));
		if ((mode == null) || (!mode.equalsIgnoreCase("parallel"))) 
			mode = "normal"; 
		
		/* <mvsinfo jcl="value" step="value" /> */
		OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
		if (mvsInfo == null)
			throw new TaskcontrollerParserException("Error, mvsinfo tag is null or empty.", ErrorType.ERROR);
			
		String jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
		String step = mvsInfo.getAttributeValue(new QName("", "step"));
		
		OMElement bertaParams = ome.getFirstChildWithName(new QName("", "bertaParams"));
		if (bertaParams == null)
			throw new TaskcontrollerParserException("Error, bertaParams tag is null or empty.", ErrorType.ERROR);
			
		String environment = bertaParams.getAttributeValue(new QName("", "environment"));
		String name = bertaParams.getAttributeValue(new QName("", "name"));
		
		DefaultParam[] parameter = { new DefaultParam("mode", mode), 
									 new DefaultParam("jcl", jcl), 
									 new DefaultParam("step", step),
									 new DefaultParam("environment", environment),
									 new DefaultParam("name", name)};
		return parameter;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement result = factory.createOMElement("result", name);
		
		OMElement rc = factory.createOMElement("rc", name);
		String gridrc = (String)input.get("gridrc");
		String exitStatusCode = (String)input.get("exitStatusCode");
		rc.addAttribute("value", gridrc, name);
		rc.addAttribute("finish", exitStatusCode, name);
		
		result.addChild(rc);
		return result;
	}
}