package com.imcs.grid.taskcontroller;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import org.apache.axis2.AxisFault;

import com.imcs.grid.session.GridSessionContainer;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class ServiceExecution {
	
	private static GridLog logger = GridLogFactory.getInstance().getLog(ServiceExecution.class); 
	
	private boolean stop = false;
	private TaskControllerCommand activeStep = null;
	
	public Action process(Service service, Action action, JobState status) throws TaskControllerException {		
		SrvMng srvMng = SrvMng.getDefault();
		Long initTime = null;
		Command command = null;
		Step step = service.getStep(0);
		boolean nexStepAvalaible = true;
		TaskControllerCommand stepExecutor = null;
		List<Mapping> mappings = null;
//		TaskControllerEventEngine eventEngine = TaskControllerEventEngine.getInstance();
		while (nexStepAvalaible && !stop) {
			action.setForwards(step.getForwards());
			command = step.getCommand();
			
			stepExecutor = srvMng.getCommandInstance(command.getId());
			activeStep = stepExecutor;
			try {
				stepExecutor.checkInput(action);
			} catch (TaskControllerException e) {
				logger.error("CheckInput exception detected " + service + "\n" + action, e);
				throw e;
			}
			
			mappings = step.getMappings();
			if (mappings != null && mappings.size() > 0)
				processMappings(action, mappings);
		
			String pidGrid = status.getExecutionThread().getPid();
			String idSession = status.getExecutionThread().getSessionId();
			logger.info("CHECKMDS " + step.getId() + " " + pidGrid);

			String idExecution = Long.toString(System.nanoTime());
			action.getRequest().put("idService", service.getId());
			action.getRequest().put("idStep", step.getId());
			action.getRequest().put("idExecution", idExecution);
			
			String jclStep = (String)action.getRequest().get("step");
			if (UtilString.isNullOrEmpty(jclStep))
				jclStep="";
			
			if (idSession.equals(GridSessionContainer.NO_SESSION)) { 
				String jcl = (String)action.getRequest().get("jcl");
				if (UtilString.isNullOrEmpty(jcl))
					jcl = "";
				jclStep = jcl + " - " + jclStep;
			}
			//eventEngine.callAddExecution(idExecution,step.getId(),Node.getMyself().getLocation(),idSession,service.getId(),pidGrid,jclStep);
			try {
				initTime = System.currentTimeMillis();
				TaskControllerToBrokerClient.callAddExecution(idExecution,step.getId(),Node.getMyself().getLocation(),idSession,service.getId(),
						pidGrid,jclStep,getStringDate());
			} catch (AxisFault a) {
				logger.error("Error in callAddExecution :: " +idExecution+" ::: "+step.getId() +" ::: "+Node.getMyself().getLocation()+" ::: "+idSession+" ::: "
						+ service.getId()+" ::: "+pidGrid+" ::: "+jclStep, a);
			} catch (InterruptedException ie) {
				logger.error("Error in callAddExecution :: " +idExecution+" ::: "+step.getId() +" ::: "+Node.getMyself().getLocation()+" ::: "+idSession+" ::: "
						+ service.getId()+" ::: "+pidGrid+" ::: "+jclStep, ie);
			}
			String pid = (String)action.getRequest().get("pid");
			logger.info(">>>>>>>>>>>>>>> INIT ["+pid+"][" + service.getId() +"]["+step.getId()+"]" );
			Forward forward = stepExecutor.execute(action, step.getParameters(), status);
			logger.info(">>>>>>>>>>>>>>> END ["+pid+"][" + service.getId() +"]["+step.getId()+"]" );
			try {
				TaskControllerToBrokerClient.callEndExecution(idExecution,pidGrid,getStringDate());				
			} catch (AxisFault a) {
				logger.error("Error in callEndExecution :: idExecution :: " + idExecution + " pidGrid " + pidGrid, a);
			} catch (InterruptedException ie) {
				logger.error("Error in callEndExecution :: idExecution :: " + idExecution + " pidGrid " + pidGrid, ie);
			}
			
			try {
				TaskControllerToBrokerClient.callAddGridAvailability(pid.split(":")[1].substring(0, pid.split(":")[1].lastIndexOf("_")), Node.getMyself().getLocation(), null, initTime, pid.split(":")[2]);
			} catch (AxisFault a) {
				logger.error("Error in callEndExecution :: idExecution :: " + idExecution + " pidGrid " + pidGrid, a);
			} catch (InterruptedException ie) {
				logger.error("Error in callEndExecution :: idExecution :: " + idExecution + " pidGrid " + pidGrid, ie);
			}
//			eventEngine.callEndExecution(idExecution,pidGrid);
			if (forward == null)
				nexStepAvalaible = false;
			else if (forward.isGlobal())
				step = new Step("global", srvMng.getGlobal(forward.getToStep()));
			else
				step = service.getNextStep(forward);
		}
		return action;
	}

	private void processMappings(Action action, List<Mapping> mappings) {
		Response response = action.getResponse();
		Request request = action.getRequest();
		for (Mapping mapping : mappings) {
			if (mapping.getTo() != null && response.get(mapping.getFrom()) != null)
				request.put(mapping.getTo(),response.get(mapping.getFrom()));
		}
		Object pid = response.get("pid");
		if (pid != null)
			request.put("pid",response.get("pid"));
	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		if (stop)
			activeStep.setStop(stop);
		this.stop = stop;
	}
	
	private String getStringDate() {
		Date today = new java.util.Date();
		Timestamp dateTimestamp = new Timestamp(today.getTime());
		String date = dateTimestamp.toString().replaceAll("-","").replaceAll(" ","").replaceAll(":","");
		return date;
	}
}
