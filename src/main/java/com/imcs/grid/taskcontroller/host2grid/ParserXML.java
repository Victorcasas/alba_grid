package com.imcs.grid.taskcontroller.host2grid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.om.OMNamespace;

public class ParserXML {	
	
	public static OMElement textToXML(String text) {
		OMFactory factory= OMAbstractFactory.getOMFactory();
		OMNamespace poNs= factory.createOMNamespace("http://grid.imcs.com/taskcontroller", "");		
		OMElement element = null;
		
		StringTokenizer tokenText = new StringTokenizer(text,"|");
		int nData = tokenText.countTokens();
		String[] arrayText = new String[nData];
		int count = 0;        
		
		while (tokenText.hasMoreTokens()){
			arrayText[count]=tokenText.nextToken();                        
			count++;
		}
		List<OMElement> vNodes=new ArrayList<OMElement>();
		String parentNodeName = "";
		OMElement parentNode = null, elem_aux = null;
		boolean enc;
		for (int i=0, j=0,iHasta=arrayText.length;i<iHasta;i++){
			while ((arrayText[i].indexOf(".") > 0) && ((arrayText[i].indexOf(".") < arrayText[i].indexOf("((")) || (arrayText[i].indexOf("((")<0))) {
				parentNodeName = arrayText[i].substring(0,arrayText[i].indexOf("."));				
				j = vNodes.size();
				enc = false;
				
				while ((j>0) && (!enc)) {
					elem_aux = (OMElement)vNodes.get(j-1);
					if (parentNodeName.equals(elem_aux.getLocalName()))
						enc = true;
					else
						j--;
				}
				parentNode = elem_aux;							
				arrayText[i]=arrayText[i].substring(parentNodeName.length()+1,arrayText[i].length());
			}
			if (arrayText[i].indexOf("((") > 0)  {						
				String nodeName = arrayText[i].substring(0,arrayText[i].indexOf("(("));
				element = factory.createOMElement(nodeName, poNs); 				
				
				if (parentNode != null) 					
					parentNode.addChild(element);
				
				arrayText[i] = arrayText[i].substring(nodeName.length());
				
				while (arrayText[i].indexOf("((") >= 0) {	
					int posFinalValue;
					if (arrayText[i].substring(arrayText[i].indexOf("((")+2,arrayText[i].indexOf("))")).equals("texto")) {						
						if (arrayText[i].indexOf(";")>=0)
							posFinalValue = arrayText[i].indexOf(";");
						else
							posFinalValue = arrayText[i].length();						
						String text_aux = arrayText[i].substring(arrayText[i].indexOf("=")+1,posFinalValue);
						element.setText(text_aux);																		
					}
					else {
						int posInit = arrayText[i].indexOf("((") + 2;
						int posFinal = arrayText[i].indexOf("))");
						String attr = arrayText[i].substring(posInit,posFinal);
						int posInitValue = arrayText[i].indexOf("=") + 2;												
						if (arrayText[i].indexOf(";")>=0)
							posFinalValue = arrayText[i].indexOf(";");
						else
							posFinalValue = arrayText[i].length();
						
						String value = arrayText[i].substring(posInitValue,posFinalValue-1);																		
						element.addAttribute(attr,value,poNs);																
					}					
					if (arrayText[i].indexOf(";")>=0)
						arrayText[i] = arrayText[i].substring(posFinalValue+1);
					else
						arrayText[i] = arrayText[i].substring(posFinalValue);
				}								
			}				
			else {			
				String nodeName = arrayText[i]; 
				element = factory.createOMElement(nodeName, poNs);
				if (parentNode != null) {
					parentNode.addChild(element);
				}		
			}	
			vNodes.add(element);
		}
		return vNodes.get(0);		
	}
	
	public static String XMLToTextRec(OMElement node, Stack<String> st) {
		StringBuilder text = new StringBuilder("");
		for (int i=0,iHasta=st.size();i<iHasta;i++)     	
			text.append(st.elementAt(i) + ".");    		    		
		
		text.append(node.getLocalName());
		st.push(node.getLocalName());
		
		if (node.getAllAttributes() != null)  
			text.append(getAttributes(node));    	
		if (!node.getText().trim().equals(""))
			text.append("((texto))=\"" + node.getText() + "\"");
		
		text.append("|");    	    
		Iterator<?> children = node.getChildElements();
		OMElement node_aux;
		while (children.hasNext())  {
			node_aux = (OMElement) children.next();
			if (node_aux.getType() !=3) {
				text.append(XMLToTextRec(node_aux,st));
				st.pop();
			}
		}
		return text.toString();
	}
	
	public static String getAttributes(OMElement node) {
		
		StringBuilder attr, textAttr = new StringBuilder("");
		Iterator<?> listAtributos = node.getAllAttributes();
		OMAttribute attrs;
		while (listAtributos.hasNext()) {
			attrs = (OMAttribute)listAtributos.next();    		
			attr = new StringBuilder("((" + attrs.getLocalName() + "))=\"");
			attr.append(attrs.getAttributeValue() + "\"");

			if (listAtributos.hasNext()) 
				textAttr.append(attr + ";");
			else
				textAttr.append(attr);			
		}    	    	    	
		return textAttr.toString();    	
	}       
}
