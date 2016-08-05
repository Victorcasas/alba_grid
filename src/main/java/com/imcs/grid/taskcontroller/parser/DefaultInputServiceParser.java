package com.imcs.grid.taskcontroller.parser;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.DefaultParam;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public abstract class DefaultInputServiceParser implements ServiceInputParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(DefaultInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Parse request " + inputs.length);
		Request req = new Request();
		DefaultParam param = null;
		
		for (Object obj : inputs) {
			if (obj instanceof DefaultParam) {
				param = (DefaultParam)obj;
				req.put(param.getKey(),param.getValue());
			} else {
				logger.info("Detected not default param " + obj);
				req.put(obj.toString(),obj);
			}
		}
		logger.info("Created request  ");
		logger.debug("Request " + req);
		return req;
	}
	
	public abstract OMElement responseParse(Response input) throws TaskControllerException;
}