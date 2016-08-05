package com.imcs.grid.taskcontroller.commands.commons;

import java.io.File;
import java.util.Properties;

import com.imcs.grid.cache.CachePolicy;
import com.imcs.grid.cache.GridCache;
import com.imcs.grid.cache.GridCacheFactory;
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.FileInfo;
import com.imcs.grid.util.UtilString;

public class IOLogicalCommand extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		try {
			String mode = (String)action.getRequest().get("mode");
			logger.info("INIT - Mode " + mode);
			if (!UtilString.isNullOrEmpty(mode)) {
				if ("parallel".equals(mode)) {
					GridCache cache = GridCacheFactory.getInstance(CachePolicy.LESS_DOWNLOAD_EFFORT);
					cache.processDelete();
				}
			}
			logger.info("IOLogicalCommand init 1 " + action + " **");
			logger.info("IOLogicalCommand init 2 " + action.getRequest() + " **");
			
			Object oFiles = action.getRequest().get("files");
			if (oFiles == null) {
				logger.error("FileInfo[] = null");
				throw new TaskControllerException("FileInfo[] = null",ErrorType.ERROR);
			}
			FileInfo[] files = (FileInfo[])oFiles;
			logger.debug("FilesInfo :: " + files);
			logger.debug("FilesInfo Length :: " + files.length);
			String pathDatain = (String) action.getRequest().get("path.datain");
			String pathDataout = (String)action.getRequest().get("path.dataout");
			logger.debug("pathDatain :: " + pathDatain + " ; pathDataout :: " + pathDataout);
			String[] paths = new String[] { pathDatain, pathDataout};
			
			long[] weights = new long[paths.length];
			File file = null;
			for(FileInfo fileInfo: files) {
				for(int i=0, iUntil = weights.length; i<iUntil;i++) {
					file = new File(paths[i]+File.separator+fileInfo.getName());
					if (file.exists()) {
						weights[i] += file.length();
						i=weights.length;
					}
				}
			}
			String dataIn = null;
			String dataOut = null;
			
			if (weights[0]>weights[1]) {
				dataIn = paths[0];
				dataOut = paths[1];
			}
			else { 
				dataIn = paths[1];
				dataOut = paths[0];
			}
			logger.info(" path.datain " + dataIn);
			logger.info(" path.dataout " + dataOut);
			action.getResponse().put("path.datain",dataIn);
			action.getResponse().put("path.dataout",dataOut);
			return action.findForward("success");
		}
		catch (Throwable e) {
			logger.error("Error executing IOLogical",e);
			return action.findForward("error");
		}
		finally {
			logger.info("END");
		}
	}

	public void checkInput(Action action) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}