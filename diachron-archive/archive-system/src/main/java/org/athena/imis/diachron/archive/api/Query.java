package org.athena.imis.diachron.archive.api;

/**
 * 
 * Instances of this class represent DIACHRON queries.
 *
 */
public class Query {
	
	private String queryText;
	private String queryType;			
	
	/**
	 * Sets the query text of this Query object.
	 * 
	 * @param queryText A String of the query text to be associated with this Query object.
	 */
	public void setQueryText(String queryText){
		this.queryText = queryText;
	}
	
	/**
	 * Fetches the query text associated with this Query object.
	 * 
	 * @return The query text associated with this Query object.
	 */
	public String getQueryText(){
		return this.queryText;
	}
	
	/**
	 * Sets the query type of this Query object.
	 *  
	 * @param queryType A String query type, can be "SELECT" or "CONSTRUCT"
	 */
	public void setQueryType(String queryType){
		this.queryType = queryType;
	}
	
	/**
	 * Fetches the query type associated with this Query object.
	 * 
	 * @return A String of the query type associated with this Query object.
	 */
	public String getQueryType(){
		return this.queryType;
	}

}
