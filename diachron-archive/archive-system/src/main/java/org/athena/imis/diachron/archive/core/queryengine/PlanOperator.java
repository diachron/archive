package org.athena.imis.diachron.archive.core.queryengine;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;

@SuppressWarnings("unused")
public abstract class PlanOperator {

	private Query primaryChildPart;
	private Query secondaryChildPart;
	
	public abstract ArchiveResultSet execute();
}
