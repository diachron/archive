package org.athena.imis.diachron.archive.api;

import org.athena.imis.diachron.archive.core.loggers.QueryLogger;
import org.athena.imis.diachron.archive.core.queryengine.QueryPlan;

/**
 * 
 * Implementation of the DataStatement interface for Diachron-specific queries.
 *
 */
class DiachronQueryStatement implements QueryStatement {
	
	/**
	 * 
	 */
	protected DiachronQueryStatement(){
	}
	
	/**
	 * Executes a query defined in the Query object and returns the results in the form of an 
	 * ArchiveResultSet object.
	 * 
	 * @param query The Query to be executed. 
	 */
	public ArchiveResultSet executeQuery(Query query) {
		
		QueryLogger.getInstance().log(query);
		QueryPlan plan = new QueryPlan(query);
		return plan.execute();
	}

}
