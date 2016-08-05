package com.imcs.grid.taskcontroller;


import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.imcs.grid.error.ErrorType;
import com.imcs.grid.kernel.GridConfiguration;
import com.imcs.grid.kernel.config.ConfigurationParam;
import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class Parser {
	
	private Document doc = null;
	private static Document stableVersion = null;
	private GridLog logger = GridLogFactory.getInstance().getLog(Parser.class);
	
	public Parser(String file) throws SAXException, IOException, ParserConfigurationException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(false);
		doc = factory.newDocumentBuilder().parse(new File(file));
	}

	public Map<String,Command> getCommands() throws Throwable {
		String xpath = "/srv-mng/commands/command";
		NodeList nodeCommands = XPathAPI.selectNodeList(doc,xpath);
		Command command = null;
		Element nodeCommand = null;
		Map<String,Command> commands = new Hashtable<String,Command>(8, 0.3f);
		
		for (int i=0, intServices = nodeCommands.getLength(); i < intServices ; i++ ) {
			nodeCommand = (Element)nodeCommands.item(i);
			command = new Command(nodeCommand.getAttribute("id"), nodeCommand.getAttribute("class"));
			commands.put(command.getId(),command);
		}
		return commands;
	}
	
	public Hashtable<String,Service> getServices(Map<String,Command> commands) throws Throwable {
		String xpath = "/srv-mng/service";
		NodeList nodeServices = XPathAPI.selectNodeList(doc,xpath);
		Hashtable<String,Service> services = new Hashtable<String,Service>(8, 0.3f);
		Service service = null;
		Element nodeService = null;
		for (int i=0, intServices = nodeServices.getLength(); i < intServices ; i++ ) {
			nodeService = (Element)nodeServices.item(i);
			service = new Service(nodeService.getAttribute("id"),nodeService.getAttribute("desc"),nodeService.getAttribute("summary"));				
			service.setInputParserClass(nodeService.getAttribute("inputparser"));
			parseCommandInService(commands, service, nodeService);
			services.put(service.getId(),service);
		}
		return services;
	}
	
	private void parseCommandInService(Map<String, Command> commands, Service service, Element nodeService) throws Throwable {
		String xpath = "step";
		NodeList commandsInService = XPathAPI.selectNodeList(nodeService,xpath);
		Element nodeCommand = null;
		Command command = null;
		for (int j=0, intCommands = commandsInService.getLength(); j < intCommands ; j++ ) {
			nodeCommand = (Element)commandsInService.item(j);
			command = commands.get(nodeCommand.getAttribute("command"));
			if (command == null) {
				String messageError = "Wrong command in service " + service.getId() + " step: " + nodeCommand.getAttribute("step");
				logger.error(messageError);
				throw new TaskControllerException(messageError, ErrorType.ERROR);
			}
			Step step = new Step(nodeCommand.getAttribute("id"),command);
			xpath = "forward";
			NodeList forwards = XPathAPI.selectNodeList(nodeCommand,xpath);
			Element nodeForward = null;
			String idForw = "";
			for (int z=0, intForwards = forwards.getLength(); z < intForwards ; z++ ) {
				nodeForward= (Element)forwards.item(z);
				idForw = nodeForward.getAttribute("id");
				step.getForwards().put(idForw,new Forward(idForw,nodeForward.getAttribute("toStep")));
			}
			xpath = "mapping";
			NodeList mappings = XPathAPI.selectNodeList(nodeCommand,xpath);
			Element nodeMapping = null;
			for (int z=0, intMappings = mappings.getLength(); z < intMappings ; z ++ ) {
				nodeMapping = (Element)mappings.item(z);
				step.getMappings().add(new Mapping(nodeMapping.getAttribute("from"),nodeMapping.getAttribute("to")));
			}
			xpath = "parameter";
			NodeList parameters = XPathAPI.selectNodeList(nodeCommand,xpath);
			Element parameter = null;
			for (int z=0, inParameters = parameters.getLength(); z < inParameters ; z++ ) {
				parameter = (Element)parameters.item(z);
				step.setParameter(parameter.getAttribute("name"),parameter.getAttribute("value"));
			}
			service.addStep(step);
		}
	}
	
	public Map<String,GlobalExecution> getGlobalExecutions(Map<String, Command> commands) throws Throwable {	
		String xpath = "/srv-mng/global-execute";
		NodeList globalExecsList = XPathAPI.selectNodeList(doc,xpath);
		Map<String,GlobalExecution> res = new Hashtable<String,GlobalExecution>(8,0.5f);
		GlobalExecution global = null;
		Element nodeGE = null;
		Command command = null;
		String idCommand = "", id = "";
		for (int i=0, intGlobalExec = globalExecsList.getLength(); i < intGlobalExec ; i++ ) {
			nodeGE = (Element)globalExecsList.item(i);
			idCommand = nodeGE.getAttribute("command");
			command = commands.get(idCommand);
			id = nodeGE.getAttribute("id");
			if (command == null) {
				String messageError = "Unable to locate command " + idCommand + " for global execution " + id;
				logger.error(messageError);
				throw new TaskControllerException(messageError,ErrorType.ERROR);
			}
			global = new GlobalExecution(id,command);
			res.put(global.getId(),global);
		}
		return res;
	}

	public Map<String, String> getGlobalParams() throws Throwable {
		String xpath = "/srv-mng/globlal-params/parameter";		
		NodeList nodeGloblalParams = XPathAPI.selectNodeList(doc,xpath);
		Map<String,String> globlaParams = new Hashtable<String,String>(8, 0.3f);
		Element parameter = null;
		for (int z=0, inParameters = nodeGloblalParams.getLength(); z < inParameters ; z++ ) {
			parameter = (Element)nodeGloblalParams.item(z);
			globlaParams.put(parameter.getAttribute("name"),parameter.getAttribute("value"));
		}		
		return globlaParams;
	}

	public static void writeXmlFile(String filename,Document doc) throws Throwable {
		Source source = new DOMSource(doc);
		File file = new File(filename);
		Result result = new StreamResult(file);
		Transformer xformer = TransformerFactory.newInstance().newTransformer();
		xformer.transform(source, result);
	}
	
	public void setStableDoc() {
		try{
			doc = stableVersion;
			GridConfiguration conf = GridConfiguration.getDefault();
			String file = conf.getParameter(ConfigurationParam.TC_SERVICE_FILE);
			writeXmlFile(file,doc);
		}
		catch(Throwable e){
			logger.error("Error.setStableDoc.",e);
		}
	}
	
	public void setStableVersion(){ 
		stableVersion = doc;
	}
	
	public boolean hasStableVersion(){
		return (stableVersion != null);
	}
}
