package com.imcs.grid.taskcontroller.commands.commons;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Properties;

import com.imcs.grid.commons.processconnector.ProcessConnectorBinding;
import com.imcs.grid.commons.processconnector.ProcessConnectorMng;
import com.imcs.grid.commons.ws.ShellCommandInput;
import com.imcs.grid.commons.ws.ShellCommandOutput;
import com.imcs.grid.taskcontroller.Action;
import com.imcs.grid.taskcontroller.Forward;
import com.imcs.grid.taskcontroller.JobState;
import com.imcs.grid.taskcontroller.TaskControllerException;
import com.imcs.grid.taskcontroller.commands.TaskControllerCommand;
import com.imcs.grid.util.UtilString;

public class ShellCommand extends TaskControllerCommand {

	public Forward execute(Action action, Properties parameters, JobState status) throws TaskControllerException {
		try  {
			ShellCommandInput input = (ShellCommandInput)action.getRequest().get("args");
			String pid = (String)action.getRequest().get("pid");
			
			if (!input.getCmd().equalsIgnoreCase("NOTHING")) {
				ProcessConnectorBinding connector = new ProcessConnectorBinding(input.getWorkingDir(),input.getCmd(),
																				input.getParams(),pid);
				ProcessConnectorMng.getInstance().addProcess(pid, connector);
				ProcessConnectorMng.getInstance().startProcess(connector);
				ShellCommandOutput output = ProcessConnectorMng.getInstance().monitor(connector);
				
				action.getResponse().put("result", output);
				action.getResponse().put("recordToMds","true");
				
				if (!UtilString.isNullOrEmpty(input.getClientIp()) && !UtilString.isNullOrEmpty(input.getClientPort())) {
					String fullOuput = ProcessConnectorMng.getInstance().getFullOutput(output.getOutputPath(), "\n");
					output.setFullOuput(fullOuput);
					sendOutcomeBySocket(input,output);
				}
			}
			else 
				sendOutcomeBySocket(input,"NOTHING_ECHO");
			
			if (action.getRequest().containsKey("directProcess") && ((Boolean)action.getRequest().get("directProcess")).equals(true)) {
				return null;
			}
			return action.findForward("success");
		}
		catch(Throwable th) {
			logger.error("Error shell commnad ",th);
			return action.findForward("error");
		}
	}
	
	private void sendOutcomeBySocket(ShellCommandInput input,ShellCommandOutput out) throws Throwable {
        // Create an unbound socket
		Socket sock = new Socket(input.getClientIp(),Integer.parseInt(input.getClientPort()));
		BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
        wr.write(out.getFullOuput());
        wr.close();
        sock.close();
    }
	
	private void sendOutcomeBySocket(ShellCommandInput input,String cadena) throws Throwable {
        // Create an unbound socket
		Socket sock = new Socket(input.getClientIp(),Integer.parseInt(input.getClientPort()));
		BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(sock.getOutputStream()));
        wr.write(cadena);
        wr.close();
        sock.close();
    }

	public void checkInput(Action action) throws TaskControllerException {}
	
	public void deploy() throws TaskControllerException {}

	public void install() throws TaskControllerException {}

	public void loadConfiguration() throws TaskControllerException {}

	public void undeploy() throws TaskControllerException {}
}