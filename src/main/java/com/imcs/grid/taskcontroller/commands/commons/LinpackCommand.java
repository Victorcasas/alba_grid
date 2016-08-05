package com.imcs.grid.taskcontroller.commands.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.Response;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;

public class LinpackCommand extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		try {
			String linPackIterations = (String)action.getRequest().get("linpack.iterations.number");
			logger.debug("linpack.iterations.number " + linPackIterations);
			int iterations = Integer.parseInt(linPackIterations);
			
			Linpack linpackBenchmarck =  new Linpack();
			List<double[]> results = new ArrayList<double[]>(iterations);
			
			long timeStart = System.currentTimeMillis();
			
			logger.info("******************************************************");
			logger.info("[PROCESSING] Init linpack - Iterations :: " + iterations);
			logger.info("******************************************************");

			for (int i = 0; i < iterations; i++) {
				double res[] = linpackBenchmarck.run_benchmark(); 
				results.add(res);
			}
			double[] finalRes = new double[4]; 
			for (double[] partialres : results ) {
				finalRes[0] += partialres[0];
				finalRes[1] += partialres[1];
				finalRes[2] += partialres[2];
				finalRes[3] += partialres[3];
			}
			finalRes[0] += finalRes[0]/iterations;
			finalRes[1] += finalRes[1]/iterations;
			finalRes[2] += finalRes[2]/iterations;
			finalRes[3] += finalRes[3]/iterations;
			
			logger.info("******************************************************");
			logger.info("[PROCESSING] End linpack ");
			logger.logTime("[PROCESSING] Time",timeStart ,System.currentTimeMillis());
			logger.info("******************************************************");
			
			Response response = action.getResponse();
			response.put("linpack.mflops.avg", finalRes[0] );
			response.put("linpack.time.avg", finalRes[1]);
			response.put("linpack.residual.avg", finalRes[2]);
			response.put("linpack.precision.avg", finalRes[3]);
			
			return action.findForward("success");
		}
		catch (Throwable e) {
			logger.error("Error executing linpack", e);
			status.setMessageDescription("Error processing Linpack");
			return action.findForward("error");
		}
	}

	public void checkInput(Action action) throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void deploy() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}