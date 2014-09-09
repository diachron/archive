package org.athena.imis.diachron.archive.core.queryengine;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;

//FIXME what is the point of this class???
public abstract class PlanOperator {

	private Query primaryChildPart;
	private Query secondaryChildPart;
	
	public abstract QueryExecution execute();
}
