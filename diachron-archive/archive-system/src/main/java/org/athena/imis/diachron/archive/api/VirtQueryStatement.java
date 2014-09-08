package org.athena.imis.diachron.archive.api;

import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Implements the QueryStatement interface for Virtuoso-specific query statements.
 *
 */
class VirtQueryStatement implements QueryStatement {
	private final VirtuosoDataSource dataSource;
  
	protected VirtQueryStatement(VirtuosoDataSource dataSource){
	  this.dataSource = dataSource;
	}
	
	/**
	 * Executes a query defined in the input parameters. 
	 * @param query The query to be executed.
	 */
	public ArchiveResultSet executeQuery(Query query) {
		
		
		ArchiveResultSet ars = new ArchiveResultSet();		
	    VirtGraph graph =  new VirtGraph(this.dataSource);
	    VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query.getQueryText(), graph);
	    try {
    	    String queryType = query.getQueryType();
    		if(queryType.equals("SELECT")){		
    			ResultSet results = vqe.execSelect();						     	    
    			ars.setJenaResultSet(results);
    			return ars;
    		}
    		else if (queryType.equals("CONSTRUCT")){			
    			Model results = vqe.execConstruct();			
    			ars.setJenaModel(results);
    			return ars;
    		}
    		else {
    		  return null;
    		}
	    } finally {
	      vqe.close();
//	      graph.close(); FIXME since a resultSet is returned, this is not executable!!! FIX ASAP!
	    }
		
	}

}
