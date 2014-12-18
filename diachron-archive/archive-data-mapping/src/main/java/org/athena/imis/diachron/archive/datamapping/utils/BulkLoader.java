package org.athena.imis.diachron.archive.datamapping.utils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;

import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;

public class BulkLoader {

	private static final Logger logger = LoggerFactory.getLogger(BulkLoader.class);
	
	/**
	 * Loads the RDF data to the given named graph. 
	 * <p>
	 * The method uses the Virtuoso isql tools to load the data from a file saved in the filesystem. 
	 * So it saves the contents of the input stream to a file and passes the file name to the isql functions
	 * </p>
	 * @param stream	the input stream to read from
	 * @param graphName	the graph name to be created with the data from the input stream
	 */
	public static boolean bulkLoadRDFDataToGraph(InputStream stream, String graphName, String ext) {
		
		String fileName = "upload_file."+(new Date()).getTime()+"."+ext;
		Path path = Paths.get(StoreConnection.getBulkLoadPath()+fileName);
		
		try {
			Files.copy(stream, path);			
			Connection conn = StoreConnection.getConnection();		     
			Statement statement = conn.createStatement();			
			String deletePastUploads = "delete from db.dba.load_list where ll_state='2'";
			statement.execute(deletePastUploads);
		    String bulkLoadSetQuery = "ld_dir('"+StoreConnection.getBulkLoadPath()+"', '"+fileName+"', '"+graphName+"')";
		    System.out.println(bulkLoadSetQuery);
		    statement.execute(bulkLoadSetQuery);
		    String runBulkLoader = "rdf_loader_run()";
		    statement.execute(runBulkLoader);
		    statement.close();
		    return true;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			return false;
		}
	}
	
	/**
	 * Empties the given named graph from the store
	 * @param tempGraph the graph name to be emptied
	 */
	public static void clearStageGraph(String tempGraph) {
		/*VirtGraph graph = StoreConnection.getVirtGraph(tempGraph);	    
		graph.clear();
		graph.close();*/
		try{			
			Connection conn = StoreConnection.getConnection();											 
		    Statement stmt = conn.createStatement ();
		    stmt.execute ("log_enable(3,1)");
		    stmt.executeQuery("SPARQL CLEAR GRAPH <"+tempGraph+">");
		}catch(Exception e){
			// TODO notify someone?
			logger.error(e.getMessage(), e);
			Graph graph = StoreConnection.getGraph(tempGraph);	    
			graph.clear();
			graph.close();
		}
		
	}
}
