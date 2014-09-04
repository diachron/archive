package org.athena.imis.diachron.archive.api;


/**
 * An interface for implementing archive query statements.
 *
 */
public interface QueryStatement{
	
	public ArchiveResultSet executeQuery(Query query);

}
