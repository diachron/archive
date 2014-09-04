package org.athena.imis.diachron.archive.core.queryengine;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;

public class QueryPlan {
	private Query source;
	private PlanOperator root;
	private Object statistics;
	
	public QueryPlan(Query source) {
		this.source = source;
		root = new PlanOperator() {
			
			@Override
			public ArchiveResultSet execute() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}
	
		
	public ArchiveResultSet execute() {
		return root.execute();
	}
		
	
}
