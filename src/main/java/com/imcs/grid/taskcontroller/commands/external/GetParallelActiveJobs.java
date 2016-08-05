package com.imcs.grid.taskcontroller.commands.external;

import java.io.File;
import java.util.Calendar;
import java.util.Properties;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.UtilString;

public class GetParallelActiveJobs extends TaskControllerCommand {
	
	/**
	 * Get parallel active jobs from HOST file.
	 */
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		logger.debug("INIT - GetParallelActiveJobs execution.");
		
		String exitStatusCode = "KO";
		String exitCodeDescription = "";
		int exitCode = 0;
		
		long timeStart = System.currentTimeMillis();
		
		/* Generic executions parameters */
		String pid = (String)action.getRequest().get("pid");
		String rexxCommand = (String) action.getRequest().get("getParallelActiveJobsRexx"); 
		
		String[] paramsResult = MdsProvider.getResultParams(pid, ".xls");
		String resultReference = paramsResult[0];
		
		/* Set XLS name with date */
		Calendar c = Calendar.getInstance();
		
		String datetime = c.get(Calendar.DAY_OF_MONTH) + "-" + (c.get(Calendar.MONTH) + 1) + "-" + c.get(Calendar.YEAR);
		GridConfiguration conf = GridConfiguration.getDefault();
		String gridName = conf.getParameter(ConfigurationParam.ROOT_GRID_NAME);
		
		String urlReference = resultReference.substring(0,resultReference.lastIndexOf(File.separator))+File.separator+ gridName +"_Parallel_actives_jobs_"+datetime+".xls";
		String nameFile = paramsResult[1].substring(0,paramsResult[1].lastIndexOf((File.separator)))+File.separator+ gridName +"_Parallel_actives_jobs_"+datetime+".xls";
		String whereToSave = new File("").getAbsolutePath() + File.separator + nameFile;
		String command = (String)parameters.getProperty("command");
		try { 	
			logger.info("***************************************************************************");
			logger.info("[GETPARALLELACTIVEJOBS] Init execute getParallelActiveJobs with pid " + pid); 
			logger.info("****************************************************************************");
			
			action.getResponse().put("exitStatusCode", exitStatusCode);
			
			/* Directory where you run the command */
			String workingDir = (String) action.getRequest().get("fc-working-dir");
			if (UtilString.isNullOrEmpty(workingDir)) logger.warn("fc-working-dir is null or empty.");

			/* Call Process connector: 
			 *   workingDir : workingDir
			 *   command : rexx GRID_GetParallelActives 
			 *   params:
			 *      - targetFile. File with source files to compile
			 *   pid 
			 *    
			 *   Parameters example: "C:/GRID/1.xls"  
			 * */
			
			/* Process connector parameters for get parallel active jobs */
			String pcParameters = whereToSave + " " +command;
			
			ProcessConnectorBinding connector = new ProcessConnectorBinding(workingDir, rexxCommand, pcParameters,pid);
			ProcessConnectorMng.getInstance().addProcess(pid, connector);
			ProcessConnectorMng.getInstance().startProcess(connector);
			ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
			
			exitCode = output.getExitCode();
			exitStatusCode = output.getExitStatusCode();
			exitCodeDescription = output.getExitCodeDescription();
			
			/* Possible values of exitStatusCode: OK, KO */
			if (exitStatusCode.equalsIgnoreCase("KO")) {
				action.getResponse().put("description", exitCodeDescription);
				action.getResponse().put("gridrc", String.valueOf(exitCode));
				action.getResponse().put("mdsReference",urlReference);
				return action.findForward("error");
			}
			else if (exitStatusCode.equalsIgnoreCase("OK")) {
				action.getResponse().put("description","Service executed successfully.");
				action.getResponse().put("gridrc",String.valueOf(exitCode));
				action.getResponse().put("mdsReference",urlReference);
				return action.findForward("success");
			}
			else {
				logger.error("Error getting the exit status code for the service or exit status code is not OK or " +
						"KO -> '" + exitStatusCode + "'");
				action.getResponse().put("exitStatusCode", "KO");
				action.getResponse().put("gridrc", "9992");
				action.getResponse().put("description","Error getting the exit status code for the service.");
//				action.getResponse().put("gridrcDesc", "Error getting the exit status code for the service.");
				return action.findForward("error");
			}
		}
		catch (ProcessConnectorException pcex) {
			logger.error("Error connecting with the Process Connector", pcex);
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9996");
			action.getResponse().put("description","Error connecting with Process Connector");
//			action.getResponse().put("gridrcDesc", "Error connecting with Process Connector");
			return action.findForward("error");
		}
		catch (Throwable th) {
			logger.error("Error processing GetParallelActiveJobs.", th);
			action.getResponse().put("description","Error executing GetParallelActiveJobs service");
			action.getResponse().put("exitStatusCode", "KO");
			action.getResponse().put("gridrc", "9993");
//			action.getResponse().put("gridrcDesc", "Error executing GetParallelActiveJobs service");
			return action.findForward("error");
		}
		finally {
			logger.info("***************************************************************");
			logger.info("**************** End GetParallelActiveJobs ********************");
			logger.logTime("*** GetParallelActiveJobs Time ", timeStart, System.currentTimeMillis());
			logger.info("***************************************************************");
		}
		
	}
	
	public void deploy() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
	
	public GetParallelActiveJobs() {
		logger.debug("Constructed " + getClass().getName());
	}

	public void checkInput(Action action) throws TaskControllerException {}

}
