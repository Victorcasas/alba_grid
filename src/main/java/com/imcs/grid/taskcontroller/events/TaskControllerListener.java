package com.imcs.grid.taskcontroller.events;

import java.util.ArrayList;
import java.util.List;

public interface TaskControllerListener {
	
	public void addServices(String[] services, String[] descriptions);
	
	public void addExecution(String idExec, String exec, String idNode, String idSession,
			String idService, String pidGrid, String description);
	
	public void endExecution(String idExec, String pidGrid);
	
	public void addDataWithFiles(String idExec,String pidGrid, String type, String io, 
			List<String> filesToSave, List<String> filesCached);
	
	public void addResultParallel(String jcl,String step, String nameFile1, 
			String nameFile2, String result, String fileCompareType,String rcActual,String rcDescActual);
	
	public void addStatisticsAlebra(String jcl,String step, String pid, String idExecution,
			double cpu_download, double elapsed_download, double cpu_upload, double elapsed_upload,
			int num_files_in, int size_files_in, int num_files_out, int size_files_out, int redundance_code, 
			ArrayList<String> cachedFiles);
	
	public void addServiceExecution(String gridpid, String session, String step, String idNode, String idService,
			String result, String rc, String rcDescription);
	
}