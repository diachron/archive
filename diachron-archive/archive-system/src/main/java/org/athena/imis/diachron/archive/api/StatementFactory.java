package org.athena.imis.diachron.archive.api;

/**
 * A factory class for creating query statements, i.e. objects of classes that implement the 
 * QueryStatement interface. 
 *
 */
public class StatementFactory {
	
	/**
	 * Creates a Virtuoso query statement.
	 * @return A new VirtQueryStatement object.
	 */
	public static QueryStatement createQueryStatement() {
		return new VirtQueryStatement();
	}
	
	/**
	 * Creates a Virtuoso data statement.
	 * @return A new VirtDataStatement object.
	 */
	public static DataStatement createDataStatement() {
		return new VirtDataStatement();
	}

}
