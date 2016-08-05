package com.imcs.grid.taskcontroller.parser;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class MvsCacheInputServiceParser extends DefaultInputServiceParser {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(MvsCacheInputServiceParser.class);
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init Parse request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null;
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement) obj;
				OMElement params = ome.getFirstChildWithName(new QName("", "param"));
				String type = params.getAttributeValue(new QName("", "type"));
				req.put("type", type);
				String jclName = params.getAttributeValue(new QName("", "jcl"));
				req.put("jclName", jclName);
				
				OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
				if (mvsInfo == null) {
					throw new TaskcontrollerParserException("Error, mvsinfo tag is null or empty.", ErrorType.ERROR);
				} 
				String jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
				req.put("jcl", jcl);
				String step = mvsInfo.getAttributeValue(new QName("", "step"));
				req.put("step", step);
				
				OMElement jclMsg = ome.getFirstChildWithName(new QName("", "JCL"));
				if(jclMsg!=null){
					String msg = jclMsg.toString();
					req.put("jclMsg", msg);
				}
			} else {
				logger.error("Detected not default param " + obj.getClass());
				req.put(obj.toString(),obj);
			}
		}
		
		logger.info("**** Created request: " + req);
		return req;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		return null;
	}
}
