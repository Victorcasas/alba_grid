package com.imcs.grid.taskcontroller;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.kernel.GridParameters;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.taskcontroller.parser.ServiceInputParser;
import com.imcs.grid.taskcontroller.parser.TaskcontrollerParserException;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class SrvMng {
	
	private Map<String,Service> services = null;
	private Map<String,Command> commands = null;
	private Map<String,TaskControllerCommand> commandsInstances = null;
	private Map<String,String> globalParams = null;
	private Map<String,GlobalExecution> globalExecutions = null;
	private Map<String,Action> finalActions = new Hashtable<String,Action>();
	private Map<String,ServiceExecution> pid2serviceExecution = new Hashtable<String,ServiceExecution>();
	private LogContext logContext = new LogContext();
	private static Parser parser;
	private static GridLog logger = GridLogFactory.getInstance().getLog(SrvMng.class);
	
	private static SrvMng instance = null;
	
	private SrvMng() {
	}
	
	public static SrvMng getDefault() {
		if (instance == null)
			instance = new SrvMng();
		return instance;
	}
	
	public void load(String workFlowFilePath) throws TaskControllerException 
	{
		long timestampInit = System.currentTimeMillis();
		try {
			new GridParameters().setGridParametersInFile(workFlowFilePath);
			logger.info("Workflow loading " + workFlowFilePath);
			parser = new Parser(workFlowFilePath);
			parseParams();
			parser.setStableVersion();
		} 
		catch (Throwable e) {
			logger.error("Error detected loading workflow ", e);
			reload();
			throw new TaskControllerException("Error detected loading workflow ",e , ErrorType.CRITICAL);
		}
		finally {
			logger.logTime("Workflow configuration loaded ", timestampInit, System.currentTimeMillis());
		}
	}
	
	public void parseParams() throws TaskControllerException {
		try {
			commands = parser.getCommands();	
			logger.info("Commands parsed \n");
			for(Command command : commands.values()) {
				logger.info(command.toString());
			}			
			
			globalParams = parser.getGlobalParams();
			logger.info("Global Params parsed \n");
			for(String paramId : globalParams.keySet()) {
				logger.info("Global param id :: " + paramId + " value :: " + globalParams.get(paramId));
			}
			
			services = parser.getServices(commands);
			logger.info("Commands parsed \n");
			for(Command command : commands.values()) {
				logger.info(command.toString());
			}
			
			globalExecutions = parser.getGlobalExecutions(commands);
			logger.info("Globals execs parsed \n");
			for(GlobalExecution globalExec : globalExecutions.values()) {
				logger.info(globalExec.toString());
			}
				
			loadCommandInstances();
		} 
		catch (Throwable e) {
			logger.error("Error detected reloading workflow ", e);
			throw new TaskControllerException("Error detected reloading workflow ",e , ErrorType.CRITICAL);
		}
	}
	
	public void reload()  throws TaskControllerException {
		try {
			logger.info("init of reload services");
			logger.info("Stable version :: "+ parser.hasStableVersion());
			if (parser.hasStableVersion()){
				parser.setStableDoc();
				parseParams();
			}
		} catch (Throwable e) {
			logger.error("Error detected reloading workflow ", e);
			throw new TaskControllerException("Error detected reloading workflow ",e , ErrorType.CRITICAL);
		}
	}
	
	private void loadCommandInstances() throws ClassNotFoundException, InstantiationException, IllegalAccessException, TaskControllerException {
		logger.info("Loading command instances");
		commandsInstances = new Hashtable<String , TaskControllerCommand>(10,0.3f);
		TaskControllerCommand wcInstance = null;
		Class<?> commandClass = null;
		String commandName = "";
		for (Command command : commands.values()) {
			logger.info("Loading ... " + command);
			commandClass = Thread.currentThread().getContextClassLoader().loadClass(command.getCommandClass());
			commandName = commandClass.getName();
			logger.debug("Find " + commandName);
			
			wcInstance = (TaskControllerCommand)commandClass.newInstance();
			logger.debug("Loaded " + commandName);
			
			wcInstance.loadConfiguration();
			logger.debug("Configuration charged for " + commandName);
			
			commandsInstances.put(command.getId(),(TaskControllerCommand)wcInstance);
			logger.debug("Stored in cache " + commandName);
		}
	}
	
	public OMElement executeService(String pid, String serviceId, Object[] inputs, JobState status) throws TaskControllerException {
		
		long timestampInit = System.currentTimeMillis();
		boolean error = false;
		OMElement ome = null;
		try {
			logger.info("******************************************************************");
			logger.info("Requested execution of serviceId :: " + serviceId + " ; pid :: " + pid);
			logger.info("******************************************************************");
			
			Service service = services.get(serviceId);
			if (service == null) {
				String messageError = "Unable to locate requested service " + serviceId;
				logger.error(messageError);
				logger.error("Identified services in node:\n " + services.toString());
				error = true;
				throw new TaskControllerException(messageError, ErrorType.ERROR);
			}
			logger.debug("Matched service " + service);

			try {				
				Class<?> inputParser = Thread.currentThread().getContextClassLoader().loadClass(service.getInputParserClass());
				Request req = ((ServiceInputParser)inputParser.newInstance()).parse(inputs);
				
				Action action = new Action();
				action.setRequest(req);
				
				req.put("pid", pid);
				action.getResponse().put("pid", pid);
								
				ServiceExecution execution = new ServiceExecution();
				pid2serviceExecution.put(pid, execution);
				
				Action actionRes = execution.process(service, action, status);
				pid2serviceExecution.remove(pid);
				finalActions.put(pid,actionRes);
				logger.debug("Size finalActions :: " + finalActions.size());
				
				ome = ((ServiceInputParser)inputParser.newInstance()).responseParse(action.getResponse());
				logger.debug("Action result :: " + actionRes);
			} 
			catch (TaskcontrollerParserException tpex) {
				logger.error("Error detected parsing service ", tpex);
				String gridpid = status.getExecutionThread().getPid();
				String location = Node.getMyself().getLocation(); 

				String jcl = getJCLFromPid(gridpid);
				
				try {
					TaskControllerToBrokerClient.callAddServiceExecution(gridpid, jcl, "", 
							location, service.getId(), "error", "502", "Error parsing service input.");
				} catch (AxisFault a) {
					logger.error("Error adding service execution :: " + gridpid + " :: "+jcl+" :: '' :: "+location
							+" :: "+service.getId()+" :: error :: 502 :: Error parsing service input.", a);
				} catch (InterruptedException e) {
					logger.error("Error adding service execution :: " + gridpid + " :: "+jcl+" :: '' :: "+location
							+" :: "+service.getId()+" :: error :: 502 :: Error parsing service input.", e);
				}
//				TaskControllerEventEngine.getInstance().callAddServiceExecution(gridpid, jcl, "", 
//						location, service.getId(), "error", "502", "Error parsing service input.");
				
				error = true;
				throw new TaskControllerException("Error detected parsing service.", tpex, ErrorType.ERROR);
			}
			catch (Throwable e) {
				logger.error("Error detected executing service ", e);
				error = true;
				throw new TaskControllerException("Error detected executing service ", e, ErrorType.ERROR);
			}
		}
		finally {
			logger.logTime("Finished execution of service (error : " + error + ")", timestampInit , System.currentTimeMillis());
		}
		return ome;
	}

	public TaskControllerCommand getCommandInstance(String id) {
		return commandsInstances.get(id);
	}

	public Command getGlobal(String id) {
		return globalExecutions.get(id).getCommand();
	}
	
	public TaskControllerLog getWorkFlowLog(String id) {
		TaskControllerLog log = logContext.getLog(id);
		return log;
	}
	
	public String[] getServicesID() 
	{
		String[] allServices = (String [])services.keySet().toArray(new String[0]);
		return allServices;
	}
	
	public String[] getDescriptionServices() 
	{
		Iterator<?> it = services.values().iterator();
		String[] allDescriptions = new String[services.values().size()];
		int i = 0;
		Service el = null;
		while (it.hasNext()) {
			el = (Service)it.next();
			allDescriptions[i] = el.getDescription();											
			i++;
		}				
		return allDescriptions;
	}
	
	public Collection<Service> getServices()  
	{
		return services.values();
	}

	public Map<String, Action> getFinalActions() {
		return finalActions;
	}

	public Map<String, String> getGlobalParams() {
		return globalParams;
	}

	public void stop(String pid) {
		logger.warn("Stoping " + pid);
		if (pid2serviceExecution.containsKey(pid)) {
			pid2serviceExecution.get(pid).setStop(true);
			pid2serviceExecution.remove(pid);
			logger.warn("Stoped " + pid);
		}
		else
			logger.warn("Not able to stop pid " + pid);
	}
	
	private String getJCLFromPid(String gridpid) {
		String[] pidWords = gridpid.split(":"); 		
		return pidWords[1];
	}
}