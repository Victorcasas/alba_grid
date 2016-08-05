package com.imcs.grid.taskcontroller.commands;

import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;

public interface TaskControllerProcessor {
	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException;
	public void checkInput(Action action) throws TaskControllerException;
}