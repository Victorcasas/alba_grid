package com.imcs.grid.taskcontroller.parser;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;

import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class LoadMVSStatisticsParser extends DefaultInputServiceParser {
	
private static GridLog logger = GridLogFactory.getInstance().getLog(LoadMVSStatisticsParser.class);
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init Parse request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			logger.debug("Parsing ... " + obj);
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				
				OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
				extractParamToReq(mvsInfo,"jcl",req);
				extractParamToReq(mvsInfo,"step",req);
				
			} else {
				logger.info("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request  ");
		logger.debug("Request " + req);
		return req;
	}
	
	private void extractParamToReq(OMElement ome,String paramName, Request req){
		
		String value = null; 
		if (ome != null) {
			value = ome.getAttributeValue(new QName("", paramName));
			logger.info(paramName+ " :: " + value);
		}
		if (value!=null)
			req.put(paramName,value);
		
	}

	public OMElement responseParse(Response input) throws TaskControllerException {	
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace ns = factory.createOMNamespace("", "");
		OMElement gridJobResponseElto = factory.createOMElement("gridjob-response", ns);			    		
		gridJobResponseElto.addAttribute("resultDescription",(String)input.get("description"),ns);
		return gridJobResponseElto;
	}
}