package com.imcs.grid.taskcontroller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.axiom.om.OMElement;

import com.imcs.grid.commons.utils.BackGroundTask;
import com.imcs.grid.error.ErrorType;
import com.imcs.grid.taskcontroller.messages.TaskControllerCreateMessages;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class TaskControllerWareHouse {

	private static GridLog logger = GridLogFactory.getInstance().getLog(TaskControllerWareHouse.class);

	private static Map<String, TaskControllerThread> jobs = null;
	private static Map<String, OMElement> submittedMessages = null;
	private static TaskControllerWareHouse instance = null;
	private static long timeElapsed = 1000 * 60 * 10; // 10 min.

	private TaskControllerWareHouse() {
		jobs = new Hashtable<String, TaskControllerThread>();
		submittedMessages = new Hashtable<String, OMElement>();
		scheduleDeleteJobs();
	}

	public static TaskControllerWareHouse getInstance() {
		if (instance == null)
			instance = new TaskControllerWareHouse();
		return instance;
	}

	private void scheduleDeleteJobs() {
		logger.info("INIT Thread to remove parallel jobs.");

		BackGroundTask bgt = new BackGroundTask(this, "removeParallelJobs");
		bgt.scheduledElapsedTimeJob(timeElapsed, false);

		logger.info("END Thread to remove parallel jobs.");
	}

	// Call by this.scheduleDeleteJobs()
	public void removeParallelJobs() {
		if (jobs.size() > 0) {
			logger.info("INIT Remove Parallel Jobs in taskcontroller memory. Total jobs: " + jobs.size());

			int deletedCount = 0;
			int totalCount = 0;
			try {
				Collection<TaskControllerThread> tcthreads = jobs.values();
				TaskControllerThread tcthread;
				String pid = "";
				for (Iterator<TaskControllerThread> itTcThreads = tcthreads.iterator(); itTcThreads.hasNext();) {
					totalCount++;
					tcthread = itTcThreads.next();

					if ((tcthread.getDateEnd() != null)
							&& ((tcthread.getDateEnd().getTime() + (timeElapsed)) < System.currentTimeMillis())) {
						pid = tcthread.getPid();
						if (jobs.containsKey(pid) && submittedMessages.containsKey(pid)) {
							itTcThreads.remove();
							jobs.remove(pid);
							submittedMessages.remove(pid);
							deletedCount++;
						}
					}
				}
			} catch (Exception ex) {
				logger.error("Error removing parallel jobs.", ex);
			} finally {
				logger.info("END Remove Parallel Jobs in taskcontroller memory. Deleted " + deletedCount + " of " + totalCount
						+ ". Total current jobs: " + jobs.size());
			}
		}
	}

	public void setJobs(String pid, TaskControllerThread t, OMElement submittedMessage) {
		jobs.put(pid, t);
		submittedMessages.put(pid, submittedMessage);
	}

	public Map<String, TaskControllerThread> getJobs() {
		return jobs;
	}

	public TaskControllerThread getTaskControllerThread(String pid) {
		return jobs.get(pid);
	}

	public OMElement getSubmittedMessage(String pid) throws TaskControllerException {
		OMElement submittedMessage = submittedMessages.get(pid);
		if (submittedMessage == null)
			throw new TaskControllerException("Pid not found or message is null", ErrorType.ERROR);

		/* Add status and phase status to message */
		TaskControllerThread thread = getTaskControllerThread(pid);
		TaskControllerCreateMessages.getSubmitttedMessage(submittedMessage, thread.getStatus().getStatus(), thread.getStatus()
				.getPhaseStatus());
		logger.info("Message with pid " + pid + " :: " + submittedMessage);
		return submittedMessage;
	}

	public List<OMElement> getAllSubmittedMessages() {
		Set<String> pids = jobs.keySet();
		List<OMElement> messages = new ArrayList<OMElement>(10);
		for (String pid : pids) {
			try {
				messages.add(getSubmittedMessage(pid));
			} catch (TaskControllerException tcex) {
			}
		}
		return messages;
	}

	public void setTerminate(String pid) {
		TaskControllerThread task = jobs.get(pid);
		task.setTerminate();
	}

	public boolean containsPid(String pid) {
		return jobs.containsKey(pid) && submittedMessages.containsKey(pid);
	}

	public void remove(String pid) {
		logger.info("Removing TaskControllerThread for pid :: " + pid);
		if (pid != null && jobs.containsKey(pid) && submittedMessages.containsKey(pid)) {
			jobs.remove(pid);
			submittedMessages.remove(pid);
			logger.debug("TASKCONTROLLER WAREHOUSE size jobs:: " + jobs.size() + " size submittedMessages:: "
					+ submittedMessages.size());
		}
	}
}