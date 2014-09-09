package eu.fp7.diachron.archive.core.queryengine;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;

// FIXME what is the point of this class???
public class QueryPlan extends PlanOperator {
	private Query sourceQuery;
	private PlanOperator root;
	private Object statistics;
	
	public QueryPlan(Query sourceQuery) {
		this.sourceQuery = sourceQuery;
		root = new PlanOperator() {
			
			@Override
			public QueryExecution execute() {
				// TODO Auto-generated method stub
				return null;
			}
		};
	}
	
		
	public QueryExecution execute() {
		return root.execute();
	}
		
	
}
