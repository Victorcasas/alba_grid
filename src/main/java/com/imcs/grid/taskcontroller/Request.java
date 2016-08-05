package com.imcs.grid.taskcontroller;

import java.util.Hashtable;
import java.util.Map;

import com.imcs.grid.util.log.GridLog;
import com.imcs.grid.util.log.GridLogFactory;

public class Request extends Hashtable<Object, Object>  {
	
	private static final long serialVersionUID = 1L;
	private static Map<String,String> globals = null;
	protected GridLog log = GridLogFactory.getInstance().getLog(Request.class);
	
	static {
		globals = SrvMng.getDefault().getGlobalParams();
	}
	
	public Object get(Object key) {
		Object res = super.get(key);
		if (res == null) {
			log.debug("Retrieving global key :: " + key + " globals :: " + globals);
			res = globals.get(key);
			log.debug("Retrieving global key :: " + key + " value :: " + res);
		}
		return res;
	}
}
