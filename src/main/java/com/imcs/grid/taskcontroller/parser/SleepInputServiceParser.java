package com.imcs.grid.taskcontroller.parser;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class SleepInputServiceParser extends DefaultInputServiceParser {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(SleepInputServiceParser.class);
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init Parse request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			logger.debug("Parsing ... " + obj);
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				
				String millis = extractParam(ome);
				if (millis!=null)
					req.put("millis",millis);
				
				String jcl = extractJCL(ome);
				req.put("jcl",jcl);
				
				String step = extractStep(ome);
				req.put("step",step);
				
			} else {
				logger.info("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request  ");
		logger.debug("Request " + req);	
		return req;
	}
	
	private String extractParam(OMElement ome) {	
		OMElement parameter = ome.getFirstChildWithName(new QName("", "sleepParams"));
		String millis = null;
		if (parameter != null) {
			millis = parameter.getAttributeValue(new QName("", "millis"));
			logger.info("Millis :: " + millis);
		}
		return millis;
	}
	
	private String extractJCL(OMElement ome) {	
		OMElement mvsinfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
		String jcl = "";
		if (mvsinfo != null) 
			jcl = mvsinfo.getAttributeValue(new QName("", "jcl"));
		return jcl;
	}
	
	private String extractStep(OMElement ome) {	
		OMElement mvsinfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
		String step = "";
		if (mvsinfo != null) 
			step = mvsinfo.getAttributeValue(new QName("", "step"));
		return step;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		return null;
	}
}