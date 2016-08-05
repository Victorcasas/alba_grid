package com.imcs.grid.taskcontroller.parser;

import java.util.List;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;

import com.imcs.grid.commons.ws.BeanToXml;
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class BeanInputParser extends DefaultInputServiceParser {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(BeanInputParser.class);
	
	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("INIT");
		Request req = new Request();
		OMElement ome = null; 
		
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				Object parsedObject = parse(ome);
				req.put("args",parsedObject);
				
			} else {
				logger.error("Detected not default param " + obj.getClass() );
				req.put(obj.toString(),obj);
			}
		}		
		logger.info("Created request");
		logger.debug("Request " + req);
		return req;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		try {
			OMFactory factory = OMAbstractFactory.getOMFactory();
			OMNamespace ns = factory.createOMNamespace("", "");
			OMElement gridJobResponseElto = factory.createOMElement("gridjob-response", ns);			    		
			Object res = input.get("result");
			
			if (res == null) return null;
			if (res instanceof List<?>) {
				List<?> resList = (List<?>)res;
				OMElement resultSet = factory.createOMElement("resultSet", ns);
				resultSet.addAttribute("size",Integer.toString(resList.size()),ns);
				for (Object obj : resList) {
					OMElement result = BeanToXml.beanToOM(obj,"result");
					resultSet.addChild(result);
				}
				gridJobResponseElto.addChild(resultSet);
			} 
			else {
				OMElement result = BeanToXml.beanToOM(res,"result");
				gridJobResponseElto.addChild(result);
			}
			//En funcion de parametro
			gridJobResponseElto.addAttribute("recordToMds",(String)input.get("recordToMds"),ns);
			return gridJobResponseElto;
		}
		catch (Throwable e) {
			throw new TaskControllerException("BeanInputParser ",e,ErrorType.ERROR);
		}
		finally {
			logger.info("END");
		}
	}
	
	private Object parse(OMElement ome) throws TaskcontrollerParserException {
		try {
			Object obj = BeanToXml.omToBean(ome.getFirstChildWithName(new QName("","bean")));
			return obj;
		}
		catch (Throwable e) {
			throw new TaskcontrollerParserException("BeanInputParser ",e,ErrorType.ERROR);
		}
	}		
}