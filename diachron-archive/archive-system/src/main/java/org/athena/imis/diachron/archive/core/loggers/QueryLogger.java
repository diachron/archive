package org.athena.imis.diachron.archive.core.loggers;

import org.athena.imis.diachron.archive.api.Query;


public class QueryLogger {
	private static final QueryLogger logger = new QueryLogger();
	//to disable creating new instances
	private QueryLogger() {
		
	}
	public static QueryLogger getInstance() {
		return logger;
	}
	
	public void log(Query query) {
		//TODO to be implemented
	}
	
	

}
