// package com.imcs.grid.taskcontroller.parser;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import javax.xml.namespace.QName;
//
//import org.apache.axiom.om.OMAbstractFactory;
//import org.apache.axiom.om.OMElement;
//import org.apache.axiom.om.OMFactory;
//import org.apache.axiom.om.OMNamespace;
//
//import com.imcs.grid.error.ErrorType;
//import com.imcs.grid.taskcontroller.Request;
//import com.imcs.grid.taskcontroller.Response;
//import com.imcs.grid.taskcontroller.TaskControllerException;
//import com.imcs.grid.taskcontroller.commands.DefaultParam;
//import com.imcs.grid.util.UtilString;
//import com.imcs.grid.util.log.GridLog;
//import com.imcs.grid.util.log.GridLogFactory;
//import com.imcs.grid.util.FileInfo;
//
//public class COSortInputServiceParser extends DefaultInputServiceParser {
//	
//	private static GridLog logger = GridLogFactory.getInstance().getLog(COSortInputServiceParser.class);	
//	
//	public Request parse(Object[] inputs) throws TaskControllerException {
//		logger.info("cosortInputServiceParser request :: " + inputs.length);
//		Request req = new Request();
//		OMElement ome = null; 
//		for (Object obj : inputs) {
//			if (obj instanceof OMElement) {
//				ome = (OMElement)obj;
//				DefaultParam[] parameter = parserCOSort(ome);
//				for (int i = 0; i < parameter.length; i++) {
//					logger.debug(parameter[i].getKey() + " " + parameter[i].getValue());
//					req.put(parameter[i].getKey(),parameter[i].getValue());
//				}
//			} else {
//				logger.error("Detected not default param " + obj.getClass() );
//				req.put(obj.toString(),obj);
//			}
//		}		
//		logger.info("Created request");
//		logger.debug("Request " + req);
//		return req;
//	}
//	
//	public OMElement responseParse(Response input) throws TaskControllerException {
//
//		OMFactory factory = OMAbstractFactory.getOMFactory();
//		OMNamespace name = factory.createOMNamespace("http://grid.imcs.com/taskcontroller", "");
//		OMElement ome = factory.createOMElement("gridjob-response", name);
//		String mode = (String)input.get("mode");
//		if (mode.equalsIgnoreCase("parallel")) {
//			ome.addAttribute("recordToMds","true",name);
//			OMElement resultCompare = factory.createOMElement("result", name);
//			String result = (String)input.get("result");
//			resultCompare.setText(result);
//			ome.addChild(resultCompare);
//			OMElement gridRc = factory.createOMElement("gridrc", name);
//			String rc = (String)input.get("gridrc");
//			gridRc.setText(rc);
//			ome.addChild(gridRc);
//		}
//		return ome;
//	}
//
//	private DefaultParam[] parserCOSort(OMElement ome) throws TaskControllerException {
//		String mode = ome.getAttributeValue(new QName("","mode"));
//		if ((mode == null) || (!mode.equalsIgnoreCase("parallel"))) { 
//    		logger.info("Mode :: Normal " );
//    		mode = "normal"; 
//    	}
//    	else
//    		logger.info("Mode :: " + mode);    		
// 
//		DefaultParam modeParam = new DefaultParam("mode", mode);
//		
//		OMElement mvsInfo = ome.getFirstChildWithName(new QName("", "mvsinfo"));
//		String jcl = null;
//		String step = null;
//		if (mvsInfo != null) {
//			jcl = mvsInfo.getAttributeValue(new QName("", "jcl"));
//			step = mvsInfo.getAttributeValue(new QName("", "step"));
//			logger.info("JCL:: " + jcl);
//			logger.info("Step:: " + step);
//		} else
//			logger.warn("MVSINFO is empty");
//
//		if (jcl == null)
//			jcl = "NombreJCL";
//		if (step == null)
//			step = "STEP";
//
//		DefaultParam jclParam = new DefaultParam("jcl", jcl);
//		DefaultParam stepParam = new DefaultParam("step", step);
//
//		//GET SYSIN
//		OMElement sysin = ome.getFirstChildWithName(new QName("", "sysin"));
//		if (sysin == null) 
//			throw new TaskControllerException("Error, Sysin is null.", ErrorType.ERROR);
//		String text_sysin = sysin.getText();
//		if (UtilString.isNullOrEmpty(text_sysin)) 
//			throw new TaskControllerException("Error, Sysin is null or empty.", ErrorType.ERROR);
//		
//		logger.info("Sysin:: " + text_sysin);
//		logger.info("Sort - INPUT");
//				
//		DefaultParam sysinParam = new DefaultParam("sysin", text_sysin);
//		
//		//GET MVSSORT
//		OMElement mvssort = ome.getFirstChildWithName(new QName("", "mvssort"));
//		if (sysin == null) 
//			throw new TaskControllerException("Error, mvssort is null.", ErrorType.ERROR);
//		String text_mvssort = mvssort.getText();
//		if (UtilString.isNullOrEmpty(text_mvssort)) 
//			throw new TaskControllerException("Error, mvssort is null or empty.", ErrorType.ERROR);
//		
//		logger.info("MvsSort:: " + text_mvssort);
//		DefaultParam mvsSortParam = new DefaultParam("mvssort", text_mvssort);
//		
//		OMElement inputs = ome.getFirstChildWithName(new QName("", "inputs"));				
//		Iterator<?> arrayLabel = inputs.getChildElements();
//		
//		List<FileInfo> listFileInfo = new ArrayList<FileInfo>();
//		while (arrayLabel.hasNext()) {
//			OMElement label = (OMElement) arrayLabel.next();
//			if (label == null) 
//				throw new TaskControllerException("Error, label is null.", ErrorType.ERROR);
//			String nameLabel = label.getAttributeValue(new QName("", "name"));
//			if (UtilString.isNullOrEmpty(nameLabel)) 
//				throw new TaskControllerException("Error, label of file is null or empty.", ErrorType.ERROR);
//			logger.info("Label Name :: " + nameLabel);
//			
//			Iterator<?> files = label.getChildElements();			
//			while (files.hasNext()) {						
//				OMElement file = (OMElement)files.next();
//				if (file == null) 
//					throw new TaskControllerException("Error, file is null.", ErrorType.ERROR);
//				String fileName = file.getAttributeValue(new QName("", "name"));
//				if (UtilString.isNullOrEmpty(fileName)) 
//					throw new TaskControllerException("Error, name of file is null or empty for label " + nameLabel, ErrorType.ERROR);
//				String lenRec = file.getAttributeValue(new QName("", "length"));
//				if (UtilString.isNullOrEmpty(lenRec)) 
//					throw new TaskControllerException("Error, length is null or empty " + fileName, ErrorType.ERROR);
//				String fileDisp = file.getAttributeValue(new QName("", "disp"));
//				
//				logger.info("filedisp :: " + fileDisp);
//				logger.info("File Name:: " + fileName);
//				logger.info("length:: " + lenRec);
//				
//				FileInfo fi = new FileInfo();
//				fi.setDisp(fileDisp);
//				fi.setLongRec(lenRec);
//				fi.setName(fileName);
//				fi.setLabel(nameLabel);
//				List<String> listVolumes = new ArrayList<String>();
//					
//				Iterator<?> volumes = file.getChildElements();
//				while (volumes.hasNext()) {
//					OMElement volume = (OMElement)volumes.next();
//					if (label == volume) 
//						throw new TaskControllerException("Error, label is null.", ErrorType.ERROR);
//					String idVolume = volume.getAttributeValue(new QName("", "id"));
//					if (UtilString.isNullOrEmpty(idVolume)) 
//						throw new TaskControllerException("Error, id Volume is null or empty.", ErrorType.ERROR);
//					logger.info("volumes:: " + idVolume);
//					listVolumes.add(idVolume);				
//				}
//				if (listVolumes.size()==0) 
//					throw new TaskControllerException("Not vols for label :: " + nameLabel, ErrorType.ERROR);
//			
//				String[] vols = (String[])listVolumes.toArray(new String[0]);
//				fi.setVolumes(vols);				
//				listFileInfo.add(fi);
//			}
//		}
//		if (listFileInfo.size()==0) 
//			throw new TaskControllerException("Not input files for SORT ", ErrorType.ERROR);
//
//		FileInfo[] fileInfo = (FileInfo[])listFileInfo.toArray(new FileInfo[0]);		
//		
//		DefaultParam filesIn = new DefaultParam("files", fileInfo);
//		
//		logger.info("Sort - OUTPUT");
//		OMElement outputs = ome.getFirstChildWithName(new QName("", "outputs"));
//		Iterator<?> labelsOutput = outputs.getChildElements(); 
//			
//		List<FileInfo> listFileInfoOutput = new ArrayList<FileInfo>();
//		while (labelsOutput.hasNext()) {
//			OMElement labelOutput = (OMElement)labelsOutput.next();
//			String nameLabelOutput = labelOutput.getAttributeValue(new QName("", "name"));
//			if (UtilString.isNullOrEmpty(nameLabelOutput)) 
//				throw new TaskControllerException("Error, label of file is null or empty.", ErrorType.ERROR);
//			logger.info("File Name:: " + nameLabelOutput);
//			
//			Iterator<?> filesOutput = labelOutput.getChildElements();			
//			while (filesOutput.hasNext()) {
//				OMElement file = (OMElement)filesOutput.next();
//				String fileName = file.getAttributeValue(new QName("", "name"));
//				if (UtilString.isNullOrEmpty(fileName)) 
//					throw new TaskControllerException("Error, name of file is null or empty.", ErrorType.ERROR);
//				String lenRec = file.getAttributeValue(new QName("", "length"));
//				if (UtilString.isNullOrEmpty(lenRec)) 
//					throw new TaskControllerException("Error, length of registry null or empty.", ErrorType.ERROR);
//				
//				logger.info("File Name:: " + fileName);
//				logger.info("length:: " + lenRec);
//				
//				FileInfo fi = new FileInfo();
//				fi.setName(fileName);
//				fi.setLongRec(lenRec);
//				fi.setLabel(nameLabelOutput);
//				
//				List<String> listVolumes = new ArrayList<String>();
//				Iterator<?> volumes = file.getChildElements();
//				while (volumes.hasNext()) {
//					OMElement volume = (OMElement)volumes.next();
//					if (volume == null) 
//						throw new TaskControllerException("Error, not volume sends.", ErrorType.ERROR);
//					String idVolume = volume.getAttributeValue(new QName("", "id"));
//					
//					if (mode.equalsIgnoreCase("parallel") && UtilString.isNullOrEmpty(idVolume)) 
//						throw new TaskControllerException("Error, id volume is null or empty.", ErrorType.ERROR);
//					logger.info("idVolume :: " + idVolume);
//					listVolumes.add(idVolume);
//				}
//				if (mode.equalsIgnoreCase("parallel") && listVolumes.size() == 0)
//					throw new TaskControllerException("Error, not vols for  " + fi.getName(), ErrorType.ERROR);
//				
//				String[] vols = (String[])listVolumes.toArray(new String[0]);
//				fi.setVolumes(vols);
//				
//				listFileInfoOutput.add(fi);
//			}
//		}
//		if (listFileInfoOutput.size()==0) 
//			throw new TaskControllerException("Not output files for SORT ", ErrorType.ERROR);
//		
//		FileInfo[] fileInfoOutput = (FileInfo[])listFileInfoOutput.toArray(new FileInfo[0]);
//		DefaultParam filesOut = new DefaultParam("filesOutput", fileInfoOutput);
//		
//		DefaultParam filesComp = new DefaultParam("filesComp", "");
//
//		DefaultParam[] parameter = {modeParam,jclParam, stepParam, sysinParam, mvsSortParam, filesIn, 
//				filesOut,filesComp};
//		
//    	return parameter;
//    }
//}