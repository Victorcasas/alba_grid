package com.imcs.grid.taskcontroller.util;

import java.util.ArrayList;

import org.apache.axis2.AxisFault;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.taskcontroller.client.TaskControllerToBrokerClient;

public class StatisticsUtils {

	/**
	 * Gets the statistics from PDM-Alebra and are inserted into the database.
	 * @param connector
	 * @param jcl
	 * @param step
	 * @param idExecution
	 * @param pid
	 * @throws ProcessConnectorException
	 * @throws AxisFault
	 * @throws InterruptedException
	 */
	public static void setAlebraStatistics(ProcessConnectorBinding connector, String jcl, String step, String idExecution,
			String pid) throws ProcessConnectorException, AxisFault, InterruptedException {
		String statistics = connector.getStatisticsAlebra();
		String[] arrResult = statistics.split(" ");
		float cpu_download = Float.parseFloat(arrResult[0]);
		float elapsed_download = Float.parseFloat(arrResult[1]);
		float cpu_upload = Float.parseFloat(arrResult[2]);
		float elapsed_upload = Float.parseFloat(arrResult[3]);
		int num_files_in = Integer.parseInt(arrResult[4]);
		long size_files_in = Double.valueOf(arrResult[5]).longValue();
		int num_cache_files_in = Integer.parseInt(arrResult[6]);
		long size_cache_files_in = Double.valueOf(arrResult[7]).longValue();
		int num_files_out = Integer.parseInt(arrResult[8]);
		long size_files_out = Double.valueOf(arrResult[9]).longValue();
		int redundance_code = Integer.parseInt(arrResult[10]);
		ArrayList<String> files_sizes = new ArrayList<String>();
		
		for (int i = 11; i < arrResult.length; i++) {
			files_sizes.add(arrResult[i]);
		}
		TaskControllerToBrokerClient.callAddStatisticsAlebra(jcl, step, pid, idExecution, cpu_download, elapsed_download, cpu_upload, 
				elapsed_upload, num_files_in, size_files_in, num_cache_files_in, size_cache_files_in, num_files_out, size_files_out, 
				redundance_code, files_sizes);
	}

}
