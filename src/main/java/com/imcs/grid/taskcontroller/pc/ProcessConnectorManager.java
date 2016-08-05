package com.imcs.grid.taskcontroller.pc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorException;
import com.imcs.grid.commons.utils.ExecuteSystemProcess;
import com.imcs.grid.commons.utils.LockFile;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.mdsprovider.MdsProvider;
import com.imcs.grid.types.Node;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

/**
 * ProcessConnectorManager manage the ProcessConnector functions since TaskController module.
 * Functions: start, ping, restart, check status...
 *
 * @author jja 
 * @since 2013-01-08
 * @version Last update 2013-01-15
 *
 */
public class ProcessConnectorManager {

	private static final transient GridLog logger = GridLogFactory.getInstance().getLog(ProcessConnectorManager.class);	
	private static final GridConfiguration conf = GridConfiguration.getDefault();
	private static final int START_RETRIES = 5, SLEEP_TIME = 500;

	private static ProcessConnectorManager instance;
	private ProcessConnectorBinding pcb;

	private static final int MAX_ITERATION_TO_CHECK_PING = 10, SLEEP_TIME_TO_CHECK_PING = 250;

	// Default values
	private final String host;
	private int port;
	private int portMax;
	private int currentPort = -1;
//	private final int PING_TIMEOUT;
	private final int pingInterval;
	private final String workingDirectory;
	private final String logFile;
	private final int nodePort;
	private boolean started = false;
	private String version = "";
	
		
	/**
	 * Private constructor. Singleton pattern.
	 * Set the PC configuration parameters obtained from XML files.
	 * Parameters: Host, port, port max, ping timeout, ping interval, working dir, log file and node port. 
	 */
	private ProcessConnectorManager() {
		
		host = conf.getParameter(ConfigurationParam.SC_BINDING_IP);
		port = currentPort = conf.getParamAsInt(ConfigurationParam.PC_BINDING_PORT);				// Port where there must be a Process Connector listening.
		portMax = conf.getParamAsInt(ConfigurationParam.PC_BINDING_PORT_MAX);		// Upper bound for ports range.
//		PING_TIMEOUT = conf.getParamAsInt(ConfigurationParam.PC_PING_TIMEOUT)   * 1000;	// Timeout for ping.
		pingInterval = conf.getParamAsInt(ConfigurationParam.PC_PING_INTERVAL) * 1000;	// Interval between pings.		
		workingDirectory = conf.getParameter(ConfigurationParam.PC_WORKING_DIR);		// Dir for logs.
		logFile = conf.getParameter(ConfigurationParam.PC_LOG_DIR) + File.separator + conf.getParameter(ConfigurationParam.PC_LOG_FILE);
		
		String strMe= conf.getParameter(ConfigurationParam.MY_LOCATION);
		nodePort = Integer.parseInt(strMe.substring(strMe.indexOf( ':')+1));
		
		logger.info("======== Process Connector Parameters ========");
		logger.info("      SC_BINDING_IP:: "+host);
		logger.info("    SC_BINDING_PORT:: "+nodePort);
		logger.info("    PC_BINDING_PORT:: "+port);
		logger.info("PC_BINDING_PORT_MAX:: "+portMax);
		logger.info("     PC_WORKING_DIR:: "+workingDirectory);
		logger.info("         PC_LOG_DIR:: "+conf.getParameter(ConfigurationParam.PC_LOG_DIR));
		logger.info("         PC_TIMEOUT:: "+conf.getParamAsInt(ConfigurationParam.PC_PING_TIMEOUT));
		logger.info("   PC_PING_INTERVAL:: "+pingInterval);
		logger.info("==============================================");
	}

	/**
	 * Return a object instance or create it if not exist.
	 * Singleton pattern with double check.
	 * @return instance the ProcessConnectorManager static instance
	 */
	public static ProcessConnectorManager getInstance() {
		if (instance == null) {
			synchronized(ProcessConnectorManager.class) {
				if (instance == null)
					instance = new ProcessConnectorManager();
			}
		}
		return instance;
	}
	
	/**
	 * Start the Process Connector system process if it is not already started.
	 * @return true if Process Connector is running, otherwise false.
	 */
	public boolean start() {
		logger.info("Starting Process Connector...");
		setPCPortFromLog();
		logger.info(String.format("Trying the first ping to current port %d (%d)", currentPort, port));
		startPing();
		if (!started) {
			// If PC not running yet then obtain a lock a file and start it.
			logger.error("First ping failed. PC no starting in log file port: " + currentPort);
			String filename = workingDirectory + "//StartPC.py";
			LockFile lock = null;
			try {
				lock = new LockFile(filename);
				lock.tryLock();				
				if (lock.isLock()) {
					logger.info("I obtain the file lock "+filename+" and I will start the PC.");					
					if (executeStartProcessConnector())
						updatePCPortFromLog();
				} else {
					// Else, I wait until other node release the lock (other node has starting the PC)
					logger.warn("I don't obtain the file lock. Other node has starting the PC and I wait to finalize it.");
					lock.waitForLock();								
				}
				logger.info("Check again if PC is just starting ... (ping)");
				startPing();
			} catch (FileNotFoundException e) {
				logger.error("Error locking file " + filename, e);
			} finally {
				if (lock != null) {
					logger.info("Release the lock file.");
					lock.release();
					lock.close();
				}
			}
		}
		
		if (started) { 
			setVersion();
			checkVersionWithNode();
			logger.info("Process Connector started in port: " + currentPort + " with version " + version);
		} else
			logger.error("Impossible to start Process Connector.");
		
		return started;
	}

	/**
	 * Set the PC version.
	 */
	private void setVersion() {
		String command = "/usr/bin/python3 " + workingDirectory + "/PCVersion.py ";
		try {
			ExecuteSystemProcess process = new ExecuteSystemProcess(command);			
			if (process.execute()) {
				String pattern = "-ver";
				String output = process.getStandardOutput();
				int verIndex = output.indexOf(pattern);
				if (verIndex != -1) {
					version = output.substring(verIndex);
					int endLineIndex = version.indexOf("\n");
					if (endLineIndex != -1)
						version = version.substring(pattern.length() + 1, endLineIndex).trim();
				}
			}
		} catch (Exception e) {
			logger.error("Error getting Process Connector version.", e);
		} finally {
			logger.info("Process Connector version: " + version);
		}	
	}
	
	/**
	 * Check PC version and node version. Both versions must be equals.
	 * If versions are different then write one alert.
	 * @see MdsProvider#writeAlert(String, String)
	 */
	private void checkVersionWithNode() {
		if (!version.equals("") && !Node.getMyself().getVersion().equals(version)) {
			logger.warn("Node version does not match with Process Connector version. Check alerts for more information.");
			String msg = String.format("Node version: %s does not match with PC version: %s .", Node.getMyself().getVersion(), version);
			MdsProvider.writeAlert(msg,"warnmsg_" + System.currentTimeMillis());
		}
	}
	
	/**
	 * Reset the PC binding configuration and create a new PC binding.
	 * PC binding is used to send a ping.
	 * @see ProcessConnectorBinding
	 */
	private void resetPCBindingConfiguration() {
		ProcessConnectorBinding.resetProcessConnectorConfiguration();
		pcb = new ProcessConnectorBinding( workingDirectory,"", host + ":" + nodePort, System.currentTimeMillis() + ":ping");
	}
	
	/**
	 * Realize a ping to determine if PC is already running.
	 * If ping is success then check that node and PC user are equals.
	 * If file log port is not obtained then no send a ping.
	 * @see {@link #resetPCBindingConfiguration()} 
	 * @see {@link #ping()}
	 */
	private void startPing() {
		if (currentPort != -1) { // If any port isn't found in log file
			int iterations = MAX_ITERATION_TO_CHECK_PING;
			while (!started && (iterations > 0)) {
				resetPCBindingConfiguration();
				ping();
				if (started) {
					if (!validateNodeAndPCUser()) {
						started = false;
						logger.error("Process Connector and Node user are different and must be equals.");
					}
				}
				try {
					Thread.sleep(SLEEP_TIME_TO_CHECK_PING);
				} catch (InterruptedException e) {
					logger.error("Interrupted sleep in the opereation of checking ping to Process Connector");
				}
				iterations--;
			}
		}
	}
	
	/**
	 * Send a ping to PC.
	 * @see com.imcs.grid.commons.processconnector.ProcessConnectorBinding#pingProcess()
	 */
	public void ping() {		
		try {			
			pcb.pingProcess();
			started = true;
		} catch (Exception e) {
			logger.error("Error send ping to process connector.", e);
			started = false;
		}
	}
	
	/**
	 * Return a thread to send PC ping in a background thread.
	 * @return thread the thread to send a background PC ping.
	 */
	public Thread getPingThread() {
		return new Thread() {
				public void run() {
					setName("PCM_pingThread");
					ping();
				}
			};
	}
	
	public boolean validateNodeAndPCUser() {
		String pcSystemUser = "";
		try {
			pcSystemUser = pcb.getSystemUser().trim();
		} catch (ProcessConnectorException e) {
			logger.error("Error send getSystemUser to process connector. ", e);
		}
		String nodeSystemUser = System.getProperty("user.name", "");
		boolean valid = nodeSystemUser.equals(pcSystemUser);
		logger.info(String.format("Node (%s) and PC user (%s) must be equals: %b", nodeSystemUser, pcSystemUser, valid));			
		 		
		return valid;
	}
	
	/**
	 * Checks which port the process connector is listening to. The port is written into the .log file, located
	 * in ConfigurationParam.PC_LOG_DIR. This method looks for a line that looks like:
	 * |XMLRPC_SERVER| Server init ip:[IP]:[PORT]
	 * If there is a serious problem with starting up the Process Connector, the log file won't be generated, and 
	 * a exception will be raised. 
	 */
	private void setPCPortFromLog() {
		logger.info("Reading last PC port from log " + logFile+" ...");
		File fileLog = new File(logFile);
		if (fileLog.exists()) {
			BufferedReader input = null;
			String line = null;
			try {
				input = new BufferedReader(new FileReader( fileLog));
				while ((line = input.readLine()) != null && currentPort == -1) {
					if (line.lastIndexOf( "|XMLRPC_SERVER| Server init ip") != -1) {
						currentPort = Integer.parseInt(line.split(":")[2].trim().split(" ")[0]);
						conf.setParamString(ConfigurationParam.PC_BINDING_PORT, Integer.toString(currentPort));
						logger.info("PC port found in log: " + currentPort);
					}
				}
				input.close();
			} catch (Exception e) {
				logger.error("Error reading last PC port from log. The log is not formatted as supposed: "+line, e);
				input = null;
			}
		} else
			logger.warn("PC log file do not exist. Impossible to get the last PC port.");
 	}
	
	/**
	 * Execute the system command to start a PC server. 
	 * @return true if start PC command has been executed, otherwise false
	 */
	private boolean executeStartProcessConnector() {
		conf.setParamString( ConfigurationParam.PC_BINDING_PORT, Integer.toString(port));	
		ProcessConnectorBinding.resetProcessConnectorConfiguration();
		String command = getStartSystemCommand();
		logger.info("Start system command :: " + command);
		ExecuteSystemProcess process = new ExecuteSystemProcess(command);
		process.setCaptureOutput(false);

		return process.executeWithRetries(START_RETRIES);		
	}
	
	/**
	 * Update the last used PC port obtained from log file.
	 */
	private void updatePCPortFromLog() {		
		int retries = 0;
		currentPort = -1;
		do {
			setPCPortFromLog();
			if (currentPort == -1) {
				try {
					Thread.sleep(SLEEP_TIME);
				} catch (InterruptedException e) {
					logger.error("Error sleeping to retry a ping process connector.", e);
				}
			}
		} while (currentPort == -1 && retries++ < START_RETRIES );
	}
	
	/**
	 * Return a command string to start the PC. 
	 * @return command the start PC system command
	 */
	private String getStartSystemCommand() {
		// TODO Parametrize python3 path
		String prefix = "/usr/bin/python3 "+workingDirectory+"/StartPC.py";
		String ports = " -p " + port + " -pm " + portMax;
		String logfile = " -l " + logFile;
		String version = " -ver " + Node.getMyself().getVersion();
		
		return prefix + ports + logfile + version;	
	}

	/**
	 * If the PC is started.
	 * @return true if PC is started, otherwise false.
	 */
	public boolean isStarted() {
		return started;
	}

	/**
	 * Set started value
	 * @param value True or false
	 */
	public void setStarted(boolean value) {
		started = value;
	}
	
	/**
	 * Return the ping interval time.
	 * @return  ping interval
	 */
	public int getPING_INTERVAL() {
		return pingInterval;
	}
	
	/**
	 * Set the PC port.
	 * @param port the PC port
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Set the PC MAX port range.
	 * @param portMax the maximum PC port
	 */
	public void setPortMax(int portMax) {
		this.portMax = portMax;
	}
		
}
