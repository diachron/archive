package eu.fp7.diachron.archive.core.store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.codec.digest.DigestUtils;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtModel;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;
import virtuoso.jena.driver.VirtuosoUpdateFactory;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import eu.fp7.diachron.archive.core.datamanagement.RDFDictionary;
import eu.fp7.diachron.archive.core.datamanagement.StorageOptimizer;
import eu.fp7.diachron.archive.core.datamanagement.StoreConnection;
import eu.fp7.diachron.archive.core.loggers.DataStatistics;
import eu.fp7.diachron.archive.models.DiachronOntology;

/**
 * Implementation of the Loader interface for loading data into the Virtuoso instance associated with 
 * the DIACHRON archive.
 *
 */
public class VirtuosoLoader implements Loader {
  private final VirtuosoDataSource dataSource;

  public VirtuosoLoader(VirtuosoDataSource dataSource) {
    this.dataSource = dataSource;
  }
	
	/* (non-Javadoc)
	 * @see org.athena.imis.diachron.archive.core.dataloader.Loader#loadModel(com.hp.hpl.jena.rdf.model.Model, java.lang.String)
	 */
	@Override
	public void loadModel(Model model, String namedGraph){
	    Model remoteModel = new VirtModel(new VirtGraph(namedGraph, this.dataSource));
	    try {
	      remoteModel.add(model);
	    } finally {
	      remoteModel.close();
	    }
	}
	
	/* (non-Javadoc)
	 * @see org.athena.imis.diachron.archive.core.dataloader.Loader#loadData(java.io.InputStream, java.lang.String)
	 */
	@Override
	public void loadData(InputStream stream, String diachronicDatasetURI) {
		
		String tempGraph = DiachronOntology.diachronResourcePrefix+"stageGraph/"+System.currentTimeMillis();
		Connection connection = null;
	    try {
	      connection = this.dataSource.getConnection();
	    	bulkLoadRDFDataToGraph(stream, tempGraph, connection);
	    	
	    	//split the dataset into the corresponding named graphs
			splitDataset(tempGraph, diachronicDatasetURI);
			//empty the temp graph
			
			clearStageGraph(tempGraph, connection);
			
			//update the statistics for this dataset
			DataStatistics.getInstance().updateStatistics(diachronicDatasetURI);
			
			//storage optimization
			StorageOptimizer optimizer = new StorageOptimizer(diachronicDatasetURI);
			optimizer.applyStrategy();
			
		} catch (Exception e) {			
			e.printStackTrace();
		} finally {
          try {
            connection.close();
          } catch (SQLException e) {
            // silent
          }
		}

	}

	/**
	 * Empties the given named graph from the store
	 * @param tempGraph the graph name to be emptied
	 */
	private void clearStageGraph(String tempGraph, Connection connection) {
		/*VirtGraph graph = StoreConnection.getVirtGraph(tempGraph);	    
		graph.clear();
		graph.close();*/
		try{			
		    Statement stmt = connection.createStatement ();
		    stmt.execute ("log_enable(3,1)");
		    stmt.executeQuery("SPARQL CLEAR GRAPH <"+tempGraph+">");
		} catch(Exception e){
			e.printStackTrace();
			VirtGraph graph = new VirtGraph(tempGraph, dataSource);
			graph.clear();
			graph.close();
		}
		
	}

	private void createStageGraphStreaming(InputStream stream, String graphName) {
		VirtGraph graph = new VirtGraph(graphName, this.dataSource);
	    Model remoteModel = new VirtModel(graph);
	    try {
			//InputStream is = new FileInputStream("C:/Users/Marios/Desktop/datasetGraph.rdf");
			remoteModel.read(stream,null);
		} catch (Exception e) {			
			e.printStackTrace();
		} finally {
		  remoteModel.close();
		  graph.close();
		}
	}
	
	/**
	 * Loads the RDF data to the given named graph. 
	 * <p>
	 * The method uses the Virtuoso isql tools to load the data from a file saved in the filesystem. 
	 * So it saves the contents of the input stream to a file and passes the file name to the isql functions
	 * </p>
	 * @param stream	the input stream to read from
	 * @param graphName	the graph name to be created with the data from the input stream
	 * @throws IOException 
	 * @throws SQLException 
	 */
	private void bulkLoadRDFDataToGraph(InputStream stream, String graphName, Connection connection) throws IOException, SQLException {
		String fileName = "upload_file."+(new Date()).getTime()+".rdf";
		Path path = Paths.get(StoreConnection.getBulkLoadPath()+fileName);
		
		try (Statement statement = connection.createStatement()){
			Files.copy(stream, path);
			// do the ISQL stuff
			String deletePastUploads = "delete from db.dba.load_list where ll_state='2'";
			statement.execute(deletePastUploads);
		    String bulkLoadSetQuery = "ld_dir('"+StoreConnection.getBulkLoadPath()+"', '"+fileName+"', '"+graphName+"')";		    
		    statement.execute(bulkLoadSetQuery);
		    String runBulkLoader = "rdf_loader_run()";
		    statement.execute(runBulkLoader);
			
		}
	}
	
	/**
	 * Processes the RDF triples of a diachron dataset version to be inserted in the archive. 
	 * All the triples must be present in the graph whose named as {@code tempGraph}. 
	 * Triples are going to be placed to the appropriate named graphs of the archive
	 *   
	 * @param tempGraph	the graph which contains the dataset version triples	
	 * @param diachronicDatasetURI	the URI of the diachronic dataset of which a new version is created
	 * @throws Exception
	 */
	private void splitDataset(String tempGraph, String diachronicDatasetURI) throws Exception{				
		
		VirtGraph graph = new VirtGraph(tempGraph, this.dataSource);
		
		try {
    		insertDatasetMetadata(graph, tempGraph, diachronicDatasetURI);
    		
    		//This will link the dataset version to its diachronic dataset, if this information exists in the stream.
    				
    		String query = "SELECT DISTINCT ?rs FROM <"+tempGraph+"> WHERE {?rs a diachron:RecordSet}";
    		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query, graph);
    		ResultSet results = vqe.execSelect();
    		while(results.hasNext()){
    			QuerySolution rs = results.next();
    			RDFNode recordSet = rs.get("rs");
    			insertRecordSetTriples(graph, recordSet.toString(), tempGraph);													
    		}vqe.close();		
    		query = "SELECT DISTINCT ?ss FROM <"+tempGraph+"> WHERE {?ss a diachron:SchemaSet}";
    		vqe = VirtuosoQueryExecutionFactory.create (query, graph);
    		results = vqe.execSelect();
    		while(results.hasNext()){
    			QuerySolution rs = results.next();
    			RDFNode schemaSet = rs.get("ss");
    			insertSchemaSetTriples(graph, schemaSet.toString(), tempGraph);													
    		}
    		vqe.close();		
    		insertChangeSetTriples(graph, tempGraph);
		} finally {
		  graph.close();
		}
		
	}
	
	/**
	 * Inserts the triples associated with the dataset's record set.
	 * @param graph The connection VirtGraph to the archive.
	 * @param recordSet The URI of the record set to upload.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private void insertRecordSetTriples(VirtGraph graph, String recordSet, String tempGraph){
		
		String query = "INSERT INTO <"+recordSet.toString()+"> {" +
							"<"+recordSet.toString()+"> ?p ?o" +
						"} FROM <"+tempGraph+"> WHERE " +
								"{<"+recordSet.toString()+"> ?p ?o}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query, graph);
		vqe.execSelect();
		String queryRecords = "INSERT INTO <"+recordSet.toString()+"> {?rec ?p ?o} FROM <"+tempGraph+"> WHERE {<"+recordSet.toString()+"> diachron:hasRecord ?rec . ?rec ?p ?o. }";
		VirtuosoQueryExecution vqeRec = VirtuosoQueryExecutionFactory.create (queryRecords, graph);			
		vqeRec.execSelect();
		String queryRecordAtts = "INSERT INTO <"+recordSet.toString()+"> {?att ?p ?o} FROM <"+tempGraph+"> WHERE {<"+recordSet.toString()+"> diachron:hasRecord ?rec .?rec diachron:hasRecordAttribute ?att . ?att ?p ?o. }";		
		VirtuosoQueryExecution vqeRecAtts = VirtuosoQueryExecutionFactory.create (queryRecordAtts, graph);										
		vqeRecAtts.execSelect();
	}
	
	/**
	 * Inserts the triples associated with the dataset's schema set.
	 * @param graph The connection VirtGraph to the archive.
	 * @param schemaSet The URI of the schema set to upload.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private void insertSchemaSetTriples(VirtGraph graph, String schemaSet, String tempGraph){
		
		String query = "INSERT INTO <"+schemaSet.toString()+"> {" +
							"<"+schemaSet.toString()+"> ?p ?o" +
						"} FROM <"+tempGraph+"> WHERE " +
								"{<"+schemaSet.toString()+"> ?p ?o}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query, graph);
		vqe.execSelect();
		query = "INSERT INTO <"+schemaSet.toString()+"> {" +
				"?o ?p2 ?o2" +
			"} FROM <"+tempGraph+"> WHERE " +
					"{<"+schemaSet.toString()+"> ?p ?o . ?o ?p2 ?o2}";
		vqe = VirtuosoQueryExecutionFactory.create (query, graph);
		vqe.execSelect();
		query = "INSERT INTO <"+schemaSet.toString()+"> {" +
				"?o ?p3 ?o3" +
			"} FROM <"+tempGraph+"> WHERE " +
					"{<"+schemaSet.toString()+"> ?p1 [ ?p2 ?o ]. ?o ?p3 ?o3.}";
		vqe = VirtuosoQueryExecutionFactory.create (query, graph);
		vqe.execSelect();
	}
	
	/**
	 * Inserts the triples associated with the change set defined in the input temporary graph.
	 * 
	 * @param graph The connection VirtGraph to the archive.	
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private void insertChangeSetTriples(VirtGraph graph, String tempGraph){
				
			String changeSetURI = "";
			String csURIQuery = "SELECT DISTINCT ?changeSet ?recordSet FROM <"+tempGraph+"> WHERE {" +
					"?changeSet a <"+DiachronOntology.changeSet+"> " +
					"OPTIONAL {?recordSet a <"+DiachronOntology.recordSet+">}" +
					"}";			
			VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (csURIQuery, graph);
			ResultSet results = vqe.execSelect();
			String existingRecordSet = "";
			while(results.hasNext()){
				QuerySolution rs = results.next();
				changeSetURI = rs.get("changeSet").toString();
				if(rs.contains("recordSet")){
					existingRecordSet = rs.get("recordSet").toString();
				}
			}
			vqe.close();
			String versionsQuery = "SELECT DISTINCT ?old ?new " +
					"FROM <"+tempGraph+"> WHERE " +
					"{" +
						"?change <"+DiachronOntology.oldVersion+"> ?old ; " +
								"<"+DiachronOntology.newVersion+"> ";
			if(existingRecordSet.equals("")) versionsQuery += "?new }";
			else versionsQuery += "<"+existingRecordSet+"> }";			
			vqe = VirtuosoQueryExecutionFactory.create (versionsQuery, graph);
			results = vqe.execSelect();				
			while(results.hasNext()){
				QuerySolution rs = results.next();	
				if(changeSetURI.equals(""))
					changeSetURI = "http://www.diachron-fp7.eu/resource/changeset/"+DigestUtils.md5Hex(rs.get("old").toString() + rs.get("new").toString());
				String datasetQuery = "SELECT DISTINCT ?old_dataset ?new_dataset " +
						"FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> " +
						"WHERE {" +
								"?old_dataset diachron:hasRecordSet <"+rs.get("old").toString()+"> . " +
								"?new_dataset diachron:hasRecordSet <"+rs.get("new").toString()+"> . " +
								"FILTER NOT EXISTS {" +
									"?changeSet a diachron:ChangeSet ; " +
												"<"+DiachronOntology.oldVersion+"> ?old_dataset ; " +
												"<"+DiachronOntology.newVersion+"> ?new_dataset" +
								"}" +
						"}";				
				VirtuosoQueryExecution vqeDataset = VirtuosoQueryExecutionFactory.create (datasetQuery, graph);
				ResultSet resultsDataset = vqeDataset.execSelect();
				boolean changeSetAlreadyExists = true;
				while(resultsDataset.hasNext()){
					changeSetAlreadyExists = false;
					QuerySolution rsDataset = resultsDataset.next();
					String dictionaryQuery = "INSERT INTO <"+RDFDictionary.getDictionaryNamedGraph()+"> {<"+changeSetURI+"> a <"+DiachronOntology.changeSet+"> ; <"+DiachronOntology.oldVersion+"> <"+rsDataset.get("old_dataset").toString()+"> ; <"+DiachronOntology.newVersion+"> <"+rsDataset.get("new_dataset").toString()+"> }";
					VirtuosoQueryExecution vqeDatasetInsert = VirtuosoQueryExecutionFactory.create (dictionaryQuery, graph);
					vqeDatasetInsert.execSelect();
				}vqeDataset.close();
				if(changeSetAlreadyExists) return;
				
				String changesQuery = "INSERT { GRAPH <"+changeSetURI+"> {?change ?p ?o }}" +
						"WHERE {{SELECT ?change ?p ?o FROM <"+tempGraph+"> WHERE {?change <"+DiachronOntology.oldVersion+"> <"+rs.get("old").toString()+"> ; " + 
									   "<"+DiachronOntology.newVersion+"> <"+rs.get("new").toString()+"> ; " +
									   "?p ?o FILTER(?p!=<"+DiachronOntology.oldVersion+">n && ?p!=<"+DiachronOntology.newVersion+">) . " +
									   //"OPTIONAL {?o ?pp ?oo FILTER(?pp!=co:old_version}" +								   
						"}}}";
				VirtuosoUpdateFactory.create(changesQuery, graph).exec();
				String innerChangesQuery = "INSERT { GRAPH <"+changeSetURI+"> {?inner ?p ?o}}  "+
						" WHERE {" +
								"GRAPH <"+changeSetURI+"> {" +
											"?change ?prop ?inner " +
											"GRAPH <"+tempGraph+"> {?inner ?p ?o " +
											"FILTER NOT EXISTS {" +
												"?inner <"+DiachronOntology.oldVersion+"> []" +
											"}} . " +
										"}" +								
						"}";
				VirtuosoUpdateFactory.create(innerChangesQuery, graph);
				
			}vqe.close();				
	}
	
	/**
	 * Inserts the metadata of the dataset to the archive's dictionary of datasets.
	 * @param graph The connection's VirtGraph.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 * @param diachronicDatasetURI The diachronic dataset URI provided by the user, if any. If it doesn't exist, it is looked for in the temporary graph.
	 */
	private void insertDatasetMetadata(VirtGraph graph, String tempGraph, String diachronicDatasetURI){
		
		diachronicDatasetURI = validateDiachronicDatasetURI(graph, tempGraph, diachronicDatasetURI);
		Calendar cal = GregorianCalendar.getInstance();
		String timestamp = ResourceFactory.createTypedLiteral(cal).getString();
		String query = "INSERT INTO <"+RDFDictionary.getDictionaryNamedGraph()+"> " +
				"{" +
					"?dataset ?p ?o ; <"+DiachronOntology.generatedAtTime+"> \""+timestamp+"\"^^xsd:dateTime " +
				"} " +
			"FROM <"+tempGraph+"> " +
				"WHERE {" +
					"?dataset a <"+DiachronOntology.dataset+"> ; " +
							 "?p ?o" +
						"}";		
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query, graph);		
		vqe.execSelect();
		String queryDiachronic = "INSERT INTO <"+RDFDictionary.getDictionaryNamedGraph()+"> " +
				"{" +
					"<"+diachronicDatasetURI+"> ?p ?o" +
				"} " +
				"FROM <"+tempGraph+"> " +
				"WHERE {" +					
						 "<"+diachronicDatasetURI+"> ?p ?o" +
					   "}";
		VirtuosoQueryExecution vqeDiach = VirtuosoQueryExecutionFactory.create (queryDiachronic, graph);
		vqeDiach.execSelect();		
	}
	
	/**
	 * Validates the provided diachronic dataset URI to see if the dataset exists in the dictionary.
	 * @param graph The connection's VirtGraph. 
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to be validated in the dictionary.
	 * @return A string containing the fixed URI of the diachronic dataset.
	 */
	private String validateDiachronicDatasetURI(VirtGraph graph, String tempGraph, String diachronicDatasetURI){
		
		String diachronicQuery = "SELECT ?diachronicDataset " +
				"WHERE {" +
						"GRAPH <"+tempGraph+"> " + 
							"{" +
								"?diachronicDataset <"+DiachronOntology.hasInstantiation+"> ?dataset" + 
							"}" +
							/*"GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " + 
							"{" +
								"?diachronicDataset a <"+DiachronOntology.diachronicDataset+"> " +
							"}" + */
					  "}";
		//System.out.println(diachronicQuery);
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (diachronicQuery, graph);
		ResultSet results = vqe.execSelect();
		while(results.hasNext()){
			QuerySolution rs = results.next();
			String existingDiachronicDatasetURI = rs.get("diachronicDataset").toString();
			if(!existingDiachronicDatasetURI.equals(diachronicDatasetURI)) 
			diachronicDatasetURI = existingDiachronicDatasetURI;
		}
		vqe.close();
		return diachronicDatasetURI;
	}
}
