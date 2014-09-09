package eu.fp7.diachron.archive.core.loggers;



public class QueryLogger {
	private static final QueryLogger logger = new QueryLogger();
	//to disable creating new instances
	private QueryLogger() {
		
	}
	public static QueryLogger getInstance() {
		return logger;
	}
	
	public void log(String query) {
		//TODO to be implemented
	}
	
	

}
