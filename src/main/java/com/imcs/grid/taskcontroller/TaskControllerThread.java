package com.imcs.grid.taskcontroller;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;

import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;
import com.imcs.grid.taskcontroller.client.TaskControllerToTopologyClient;
import com.imcs.grid.taskcontroller.logic.TaskControllerLogical;
import com.imcs.grid.types.xml.UtilXML;
import com.imcs.grid.util.UtilString;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerThread extends Thread {

	private static final SrvMng taskController = SrvMng.getDefault();
	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerThread.class);
	private static long numberOfJobs = 0;

	private OMElement result = null;
	private String pid = null;
	private String service = null;
	private Object[] inputs = null;
	private String mode = null; // grid or parallel
	protected JobState status = new JobState("running");
	private Date date = new Date();
	private Date dateEnd = null;
	private Action finalAction = null;
	private String deliverTo = null;
	private String sessionId = null;
	private int monitorAttemps = 0;
	private Timer timer = null;
	private boolean initialized = false; 
	
	public TaskControllerThread(String pid, String sessionId, String service, String mode, Object[] inputs) {
		status.setExecutionThread(this);
		logger.info("Pid :: " + pid + " ; Service :: " + service + " ; input.length :: "
				+ (inputs == null ? "null" : inputs.length));
		logger.info("Number of total executed jobs :: " + (++numberOfJobs));

		this.pid = pid;
		this.service = service;
		this.inputs = inputs;
		this.sessionId = sessionId;
		if (!UtilString.isNullOrEmpty(mode) && mode.equalsIgnoreCase("parallel"))
			this.mode = "parallel";
		else
			this.mode = "grid";
		// logger.debug("**********************"+this.mode);
		logger.info("********************** mode: " + this.mode);
		if (this.mode.equals("grid")) {
			setTimer();
		}
	}

	public void run() {
		boolean brokerInformed = false;
		
		try {
			// Change my own state to BUSY
			String strEvent = sessionId == null || sessionId.equalsIgnoreCase("no_session") ? "newJobWithNoSession"
					: "newJobWithSession";
			TaskControllerToTopologyClient.changeState(strEvent);
			result = taskController.executeService(pid, service, inputs, status);
			if (result != null) {
				String recordToMds = result.getAttributeValue(new QName("recordToMds"));
				if (!UtilString.isNullOrEmpty(recordToMds) && recordToMds.equals("true")) {
					String mdsPath = MdsProvider.writeJobResult(UtilXML.getDocFrom(result.toString()), pid);
					OMFactory factory = OMAbstractFactory.getOMFactory();
					OMNamespace ns = factory.createOMNamespace("", "");
					result = factory.createOMElement("gridjob-response", ns);
					result.addAttribute("recordTo", mdsPath, ns);
				}
			}
			finalAction = taskController.getFinalActions().get(pid);
			logger.info("Calling unset broker because normal termination " + pid);
			changeStateToJobDone();
			//Set executing process to false before notify finish to root to avoid ws saturation problems
			TaskControllerLogical.getInstance().setExecutingProcess(false);
			TaskControllerToBrokerClient.notifyFinishToRoot(pid);
			brokerInformed = true;

			// Delete final actions after 5000 ms
			new Thread() {
				public void run() {
					setName("TC_deleteFinalActions");
					try {
						Thread.sleep(5000);
						if (taskController.getFinalActions().get(pid) != null) {
							taskController.getFinalActions().remove(pid);
							logger.debug("Final action of pid " + pid + " deleted");
						}
					} catch (Throwable e) {
						logger.error("Error deleting final action", e);
					} finally {
						try {
							this.finalize();
						} catch (Throwable e) {
							logger.error("Error to finalize thread " + getName(), e);
						}
					}
					return;
				};
			}.start();

			if (!UtilString.isNullOrEmpty(deliverTo))
				deliver();
		} catch (Throwable t) {
			logger.error("Execution Service Error :: TaskControllerThread ", t);
			status.setStatus("error");
			changeStateToJobDone();
			//Set executing process to false before notify finish to root to avoid ws saturation problems
			TaskControllerLogical.getInstance().setExecutingProcess(false);
			if (!brokerInformed) {
				logger.info("Calling unset broker because error " + pid);
				try {
					TaskControllerToBrokerClient.notifyFinishToRoot(pid);
					brokerInformed = true;
				} catch (Exception e) {
					logger.error("Error notifying finish to the root", e);
				}
				logger.debug("OK Calling unset broker because error " + pid);
			}
			logger.info("Execution thread for process " + pid + " will be killed");
			TaskControllerLogical.getInstance().asyncronousKill(pid);
			try {
				ProcessConnectorMng.getInstance().kill(pid);
			} catch (Throwable e) {
				logger.error("Error killing process " + pid + " in ProcessConnectorMng");
			}
		} finally {
			dateEnd = new Date(System.currentTimeMillis());
			if (!brokerInformed) {
				try {
					logger.warn("Sending hard finish " + pid);
					TaskControllerToBrokerClient.notifyFinishToRoot(pid);
					logger.info("Sended hard finish " + pid);
					if (timer != null) {
						timer.cancel();
					}
				} catch (Exception e) {
					logger.error("Error hard finish ", e);
				}
			}
			logger.info("##Phase status ------->>>" + status.getPhaseStatus());
			logger.info("##Status ------->>>" + status.getStatus());
//			TaskControllerLogical.getInstance().setExecutingProcess(false);

			try {
				logger.info("Finalize TaskControllerThread for " + pid);
				this.finalize();
			} catch (Throwable e) {
				logger.error("Error to finalize thread " + getName(), e);
			}
			initialized = true;
		}
	}

	private void changeStateToJobDone() {
		try { // Change my own state to IDLE
			TaskControllerToTopologyClient.changeState("jobDone");
		} catch (Exception e) {
			logger.error("Error when changing node state. RIGHT NOW THE NODE STATE IS INCONSISTENT.");
			logger.error("THIS WILL GENERATE SERIOUS PROBLEMS WHEN TRYING TO EXECUTE A JOB IN THIS NODE.");
			logger.error("It is recommended to shut down this node and restart it again", e);
		}

	}

	private void deliver() {
		try {
			logger.debug("Delivering result to " + deliverTo);
			TaskControllerToBrokerClient.deliver(deliverTo, pid, result);
			logger.debug("Result delivered");
		} catch (Throwable e) {
			logger.error(e);
		}
	}

	public void stopTaskControllerKillThread() {
		logger.info("Stopping thread...");
		TaskControllerKillThread process = TaskControllerKillThreadWareHouse.getInstance().getProcessMonitoring(pid);
		process.stopThread();
		logger.info("Stopped thread...");
	}

	public OMElement getResult() {
		while (!initialized) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.error("Thread interrupted while waiting for result.", e);
			}
		}
		return result;
	}

	public String getLive() {
		return status.getStatus();
	}

	public String getMessageDescription() {
		return status.getMessageDescription() == null ? "" : status.getMessageDescription();
	}

	public String getLogResult() {
		return status.getLogResult() == null ? "" : status.getLogResult();
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Action getFinalAction() {
		return finalAction;
	}

	public void setTerminate() {
		status.setStatus("terminated");
	}

	public long getElapsedTime() {
		if (dateEnd == null)
			return System.currentTimeMillis() - date.getTime();
		return dateEnd.getTime() - date.getTime();
	}

	public void changePhaseStatus(String newPhaseStatus) {
		try {
			TaskControllerToBrokerClient.notifyChangePhaseToRoot(pid, newPhaseStatus);
		} catch (Exception e) {
			logger.error("Error changing phase status", e);
		}
	}

	public JobState getStatus() {
		return status;
	}

	public String getService() {
		return service;
	}

	public void setDeliverTo(String deliverTo) {
		this.deliverTo = deliverTo;
	}

	public String getPid() {
		return pid;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void kill() {
		if (timer != null) {
			timer.cancel();
		}
		/*
		 * Si se hace stop del pid del taskcontroller no sigue los pasos correctos y por tanto no se almacena en base de datos lo
		 * que ha pasado
		 */
		// taskController.stop(pid);
	}

	private void setTimer() {
		GridConfiguration conf = GridConfiguration.getDefault();
		final int maxMonitorAttemps = conf.getParamAsInt(ConfigurationParam.TC_TIMER_MAX_MONITOR_ATTEMPTS);
		final int timeOfMonitorAttemps = conf.getParamAsInt(ConfigurationParam.TC_TIMER_TIME_OF_MONITOR_ATTEMPTS);
		final String gridCancelProcess = conf.getParameter(ConfigurationParam.TC_CANCEL_PROCESS_COMMAND);
		TimerTask timerTask = new TimerTask() {
			public void run() {
				if (monitorAttemps >= maxMonitorAttemps) {
					try {
						kill();
						ProcessConnectorMng pcMng = ProcessConnectorMng.getInstance();
						Vector<Object> processParams = new Vector<Object>(1);
						processParams.add(pid);
						String pidAsync = "asyncronousProcess_" + System.currentTimeMillis();
						pcMng.asyncronousProcess(pidAsync, gridCancelProcess, processParams);
						logger.info("Kill the process because there is no monitoring from client");
						MdsProvider.writeAlert("Process '" + pid + "' killed because there is no monitoring from client",
								"errormsg_" + System.currentTimeMillis());
					} catch (Throwable e) {
						logger.error("Error during kill process", e);
					}
				} else {
					monitorAttemps++;
				}
			}
		};
		timer = new Timer();
		timer.scheduleAtFixedRate(timerTask, 0, timeOfMonitorAttemps);
	}

	public void setMonitorAttemps(int ma) {
		monitorAttemps = ma;
	}

	public Date getDateEnd() {
		return dateEnd;
	}
}