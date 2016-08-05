package com.imcs.grid.taskcontroller.commands.external;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class SortIMG extends TaskControllerCommand {
	
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		try {
			status.setPhaseStatus("Computing");
			logger.info("******************************************************");
			logger.info("[PROCESSING] Init SORT IMG ");
			logger.info("******************************************************");
			String pathFileIn = (String)action.getRequest().get("pathIN");
			String pathFileOut = (String)action.getRequest().get("pathOUT");
			if (!pathFileIn.equals("") && !pathFileOut.equals("")){
				List<String> records=readInputFile(pathFileIn);
				Collections.sort(records);
				writeOutputFile(pathFileOut,records);
			}
			status.setPhaseStatus("terminated");
			logger.info("******************************************************");
			logger.info("[PROCESSING] End SORT IMG ");
			logger.info("******************************************************");
			return action.findForward("success");
		}catch (Throwable e) {
			logger.error("Error processing SORTIMG ", e);
			status.setMessageDescription("Error processing SORT IMG");
			return action.findForward("error");
		}
	}
	
	public List<String> readInputFile(String path){
		List<String> records = new ArrayList<String>();
		File fdatain= new File(path);

		try{
			if (fdatain.exists()){
				BufferedReader input =  new BufferedReader(new FileReader(fdatain));
				String line = null; 
		        while (( line = input.readLine()) != null){
		          records.add(line);
		        }
		        input.close();
			}else{
				logger.error("File data in doesn´t exist");
			}
		}catch(Exception e){
			logger.error("Error during read input file");
		}
		return records;
	}

	public void writeOutputFile(String path,List<String> records){
		try{
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
			for (int i=0, iUntil = records.size();i<iUntil;i++){
				if (i>0) { bufferedWriter.newLine(); }
				bufferedWriter.write(records.get(i));
			}
			bufferedWriter.close();
		}catch(Exception e){
			logger.error("Error during write output file");
		}
	}
	
	public SortIMG() {
		logger.debug("Constructed " + getClass().getName());
	}
	
	public void loadConfiguration() throws TaskControllerException {
		logger.debug("Configuration loaded " + getClass().getSimpleName());
	}
		
	public void checkInput(Action action) throws TaskControllerException {}


	public void deploy() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}