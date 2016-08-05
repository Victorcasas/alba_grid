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

public class CobolInputServiceParser extends DefaultInputServiceParser {

	private static GridLog logger = GridLogFactory.getInstance().getLog(CobolInputServiceParser.class);

	public Request parse(Object[] inputs) throws TaskcontrollerParserException {
		logger.info("Init request - Length :: " + inputs.length);
		Request req = new Request();
		OMElement ome = null; 
		for (Object obj : inputs) {
			if (obj instanceof OMElement) {
				ome = (OMElement)obj;
				DefaultParam[] parameter = parserExecuteCobolMessage(ome);
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

	public DefaultParam[] parserExecuteCobolMessage(OMElement ome) throws TaskcontrollerParserException {
		
		String statistics = ome.getAttributeValue(new QName("","statistics"));
		if ((statistics == null) || (!statistics.equalsIgnoreCase("true"))) 
			statistics = "false";     		

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
		if (UtilString.isNullOrEmpty(id)) 
			throw new TaskcontrollerParserException("Error, pgm.id is null or empty.", ErrorType.ERROR);
		
		String timestampPGM = pgm.getAttributeValue(new QName("", "timestamp"));
		if (UtilString.isNullOrEmpty(timestampPGM)) 
			timestampPGM = "";

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
			throw new TaskcontrollerParserException("Error, not file inputs sends.", ErrorType.ERROR);

		List<FileInfo> inputsfiles = UtilOMElementParser.parserFiles("inputs",inputs, false);
		FileInfo[] inputsFileInfo = (FileInfo[])inputsfiles.toArray(new FileInfo[0]);
		
		/* Example:
		 * <inputMod>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * 		<label name="value">
		 * 			<file name="" length="" disp="" records=""/>
		 * 			<file name="" length="" disp="" records=""/>
		 *      </label>
		 * </inputMod> 
		 * */
		FileInfo[] modsFileInfo = null;
		OMElement inputMod = ome.getFirstChildWithName(new QName("", "inputMod"));
		if (inputMod == null) 
			logger.info("inputMod not sent");
		else {
			List<FileInfo> modfiles = UtilOMElementParser.parserFiles("mod", inputMod, false);
			modsFileInfo = (FileInfo[])modfiles.toArray(new FileInfo[0]);
		}
		
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
		OMElement outputs = ome.getFirstChildWithName(new QName("", "outputs"));
		if (outputs == null)
			throw new TaskcontrollerParserException("Error, outputs tag is null or empty.", ErrorType.ERROR);
		
		List<FileInfo> outputsFiles = UtilOMElementParser.parserFiles("outputs", outputs, false);
		FileInfo[] outputsFileInfo = (FileInfo[])outputsFiles.toArray(new FileInfo[0]);
		
		DefaultParam[] parameter = { new DefaultParam("statistics", statistics), 
									 new DefaultParam("mode", mode), 
									 new DefaultParam("jcl", jcl), 
									 new DefaultParam("step", step), 
									 new DefaultParam("id", id), 
									 new DefaultParam("timestamp", timestampPGM),
									 new DefaultParam("exe.cobol", id),
									 new DefaultParam("files", inputsFileInfo),
									 new DefaultParam("filesMod", modsFileInfo), 
									 new DefaultParam("filesOutput", outputsFileInfo), 
									 new DefaultParam("filesComp", "")};
		return parameter;
	}
	
	public OMElement responseParse(Response input) throws TaskControllerException {
		
		OMFactory factory = OMAbstractFactory.getOMFactory();
		OMNamespace name = factory.createOMNamespace("", "");
		OMElement result = factory.createOMElement("result", name);
		
		String mode = (String)input.get("mode");
		if (mode.equalsIgnoreCase("parallel")) {
			result.addAttribute("recordToMds","true",name);
			OMElement resultCompare = factory.createOMElement("resultCompare", name);
			String resultComp = (String)input.get("fileCompareResult");
			resultCompare.setText(resultComp);
			result.addChild(resultCompare);
		}
	
//		StringBuffer content = new StringBuffer();
//		try {
//			String fileDisplay = "SYSOUT";
//			if ((new File(fileDisplay).exists())) {
//				BufferedReader in = new BufferedReader(new FileReader(fileDisplay));
//				String str;
//				// Number of lines read: limit
//				int lines = 50; 
//				for (int i = 0; i < lines; i++) {
//					if ((str = in.readLine()) != null) {
//						content.append(">");
//						content.append(str);
//					}
//				}
//				in.close();
//			} 
//		} 
//		catch (IOException e) {
//			logger.error("Error responseParse ",e);
//		}
//		if (content.length() != 0) {
//			OMElement display = factory.createOMElement("display", name);
//			display.setText(content.toString());
//			result.addChild(display);
//		}
		
		OMElement rc = factory.createOMElement("rc", name);
		String gridrc = (String)input.get("gridrc");
		String exitStatusCode = (String)input.get("exitStatusCode");
		rc.addAttribute("value", gridrc, name);
		rc.addAttribute("finish", exitStatusCode, name);
		
//		try {
//	        File f = new File("filename");
//	        RandomAccessFile raf = new RandomAccessFile(f, "rw");
//	        // Read a character
//	        raf.readChar();
//	        // Seek to end of file
//	        raf.seek(f.length());
//	        // Append to the end
//	        raf.writeChars("aString");
//	        raf.close();
//	    } catch (IOException e) {
//	    	logger.error("Error responseParse ",e);
//	    }
//
//		File file = new File ("SYSOUT");
//		if (file.exists())
//			file.delete();

		result.addChild(rc);
		return result;
	}
}