package org.athena.imis.diachron.archive.api;

import org.athena.imis.diachron.archive.core.dataloader.RDFDictionary;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.lang.DiachronQuery;
import org.athena.imis.diachron.archive.lang.DiachronParserSPARQL11;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.lang.SPARQLParser;

/**
 * Implements the QueryStatement interface for Virtuoso-specific query statements.
 *
 */
class VirtQueryStatement implements QueryStatement {
	
	protected VirtQueryStatement(){
	}
	
	/**
	 * Executes a query defined in the input parameters. 
	 * @param query The query to be executed.
	 */
	public ArchiveResultSet executeQuery(Query query) {
		
		ArchiveResultSet ars = new ArchiveResultSet();		
	    //VirtGraph graph =  StoreConnection.getVirtGraph();			
		VirtGraph graph =  (VirtGraph) StoreConnection.getGraph(RDFDictionary.getDictionaryNamedGraph());
	    String queryType = query.getQueryType();
		if(queryType.equals("SELECT")){	
			// DiachronQuery is extending com.hp.hpl.jena.query.Query
			DiachronQuery dq = new DiachronQuery();
			SPARQLParser parser = new DiachronParserSPARQL11();

			dq = (DiachronQuery) parser.parse(dq, query.getQueryText());
			String serializedQuery = dq.serialize();
			System.out.println(serializedQuery);
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (serializedQuery, graph);
		    ResultSet results = vqe.execSelect();		    
			ars.setJenaResultSet(results);
			vqe.close();
			graph.close();
			return ars;
		}
		else if (queryType.equals("CONSTRUCT")){			
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query.getQueryText(), graph);
		    Model results = vqe.execConstruct();			
			ars.setJenaModel(results);
			vqe.close();
			graph.close();
			return ars;
		}
		else {
			graph.close();
			return null;
		}
		
	}

}
