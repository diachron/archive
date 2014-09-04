package org.athena.imis.diachron.archive.core.loggers;


public class DataStatistics {
	private static final DataStatistics stat = new DataStatistics();
	
	//to disable creating new instances
	private DataStatistics() {
		
	}
	public static DataStatistics getInstance() {
		return stat;
	}
	
	public void updateStatistics(String DiachronDatasetId) {
		//TODO to be implemented
	}
	
	

}
