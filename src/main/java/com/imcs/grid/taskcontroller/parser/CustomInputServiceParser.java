package com.imcs.grid.taskcontroller.parser;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.taskcontroller.Request;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.DefaultParam;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class CustomInputServiceParser extends DefaultInputServiceParser {
		
		private static GridLog logger = GridLogFactory.getInstance().getLog(CustomInputServiceParser.class);
						
		public Request parse(Object[] inputs) throws TaskcontrollerParserException {
			logger.info("Init Parse request - Length :: " + inputs.length);
			Request req = new Request();
			OMElement ome = null; 
			for (Object obj : inputs) {
				logger.debug("Parsing ... " + obj);
				if (obj instanceof OMElement) {
					ome = (OMElement)obj;
					String parameters = extractParams(ome);
					if (parameters!=null)
						req.put("parameters",parameters);
					
					DefaultParam[] info = extractInfo(ome);
					if (info != null) {
						for (int i = 0; i < info.length; i++) {
							logger.debug(info[i].getKey() + " " + info[i].getValue());
							req.put(info[i].getKey(),info[i].getValue());
						}
					}
					
				} else {
					logger.info("Detected not default param " + obj.getClass() );
					req.put(obj.toString(),obj);
				}
			}
			logger.info("Created request  ");
			logger.debug("Request " + req);	
			return req;
		}
		
		private String extractParams(OMElement ome) {	
			String result = null;
			OMElement child = ome.getFirstChildWithName(new QName("", "parameters"));
			if (child != null)
				result = child.toString();
			return result;
		}
		
		private DefaultParam[] extractInfo(OMElement ome) {
			OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
			
			String jcl = "", step="";
			
			if (mvsInfo != null) {
				jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
				step = mvsInfo.getAttributeValue(new QName("", "step"));
			}
			
			DefaultParam[] info = {
					new DefaultParam("jcl", jcl),
					new DefaultParam("step", step),
			};
			
			return info;
		}
		
		public OMElement responseParse(Response input) throws TaskControllerException {
			return null;
		}
	}
