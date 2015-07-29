package org.athena.imis.diachron.archive.core.dataloader;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.core.loggers.DataStatistics;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.RDFDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateAction;

/**
 * Implementation of the Loader interface for loading data into the Virtuoso instance associated with 
 * the DIACHRON archive.
 *
 */
class VirtLoader implements Loader {
	
	private static final Logger logger = LoggerFactory.getLogger(VirtLoader.class);
	
	/* (non-Javadoc)
	 * @see org.athena.imis.diachron.archive.core.dataloader.Loader#loadModel(com.hp.hpl.jena.rdf.model.Model, java.lang.String)
	 */
	public void loadModel(Model model, String namedGraph){
		Model remoteModel = StoreConnection.getJenaModel(namedGraph);
	    remoteModel.add(model);
	}
	
	/* (non-Javadoc)
	 * @see org.athena.imis.diachron.archive.core.dataloader.Loader#loadData(java.io.InputStream, java.lang.String, java.lang.String)
	 */
	public String loadData(InputStream stream, String diachronicDatasetURI, String format) throws Exception{
		
		long tStart = System.currentTimeMillis();
		String tempGraph = DiachronOntology.diachronResourcePrefix+"stageGraph/"+System.currentTimeMillis();
		
	    try {
	    	//TODO this is the only virtuoso dependent method, move to other class 
	    	bulkLoadRDFDataToGraph(stream, tempGraph, format);
	    	
	    	//Decide whether to store a full version or just the changeset and the dataset metadata
	    	//by using the StorageOptimizer
	    	StorageOptimizer optimizer = new StorageOptimizer(tempGraph);
	    	optimizer.applyStrategy();
	    	String datasetURI ;
	    	boolean fullyMaterialized;
	    	if(optimizer.getStrategy() == 1){
	    		fullyMaterialized = true;	    		
	    	}
	    	else{
	    		fullyMaterialized = false;	    		
	    	}
	    	
	    	datasetURI = splitDataset(tempGraph, diachronicDatasetURI, "", fullyMaterialized);
	    	
	    	//split the dataset into the corresponding named graphs
			
			
			//add the new dataset to the cache
			DictionaryService dictService = StoreFactory.createDictionaryService();
			Graph graph = StoreConnection.getGraph(RDFDictionary.dictionaryNamedGraph);
			if(null != datasetURI) {
				dictService.addDataset(graph, diachronicDatasetURI, datasetURI, fullyMaterialized);
			}
			graph.close();
			
			//empty the temp graph
			clearStageGraph(tempGraph);
			
			//update the statistics for this dataset
			DataStatistics.getInstance().updateStatistics(diachronicDatasetURI);
												
			
			return datasetURI;
		} catch (Exception e) {					
			throw e;
		}
	    /*finally {
	    	long tEnd = System.currentTimeMillis();
	    	long tDelta = tEnd - tStart;
	    	double elapsedSeconds = tDelta / 1000.0;
	    	System.out.println(elapsedSeconds);
	    }*/
		
	    
	}
	
	public String loadData(InputStream stream, String diachronicDatasetURI, String format, String versionNumber) throws Exception{
		
		long tStart = System.currentTimeMillis();
		String tempGraph = DiachronOntology.diachronResourcePrefix+"stageGraph/"+System.currentTimeMillis();
		
	    try {
	    	//TODO this is the only virtuoso dependent method, move to other class 
	    	bulkLoadRDFDataToGraph(stream, tempGraph, format);
	    	
	    	//split the dataset into the corresponding named graphs
	    	StorageOptimizer optimizer = new StorageOptimizer(tempGraph);
	    	optimizer.applyStrategy();
	    	String datasetURI ; 
	    	boolean fullyMaterialized;
	    	if(optimizer.getStrategy() == 1){
	    		fullyMaterialized = true;	    		
	    	}
	    	else{
	    		fullyMaterialized = false;	    		
	    	}
	    	datasetURI = splitDataset(tempGraph, diachronicDatasetURI, versionNumber, fullyMaterialized);
			//add the new dataset to the cache
			DictionaryService dictService = StoreFactory.createDictionaryService();
			Graph graph = StoreConnection.getGraph(RDFDictionary.dictionaryNamedGraph);
			if(null != datasetURI) {
				dictService.addDataset(graph, diachronicDatasetURI, datasetURI, fullyMaterialized);
			}
			graph.close();
			
			//empty the temp graph
			clearStageGraph(tempGraph);
			
			//update the statistics for this dataset
			DataStatistics.getInstance().updateStatistics(diachronicDatasetURI);
									
			return datasetURI;
			
		} catch (Exception e) {					
			throw e;
		}
	    /*finally {
	    	long tEnd = System.currentTimeMillis();
	    	long tDelta = tEnd - tStart;
	    	double elapsedSeconds = tDelta / 1000.0;
	    	System.out.println(elapsedSeconds);
	    }*/
		
	    
	}
	
	/* (non-Javadoc)
	 * @see org.athena.imis.diachron.archive.core.dataloader.Loader#loadData(java.io.InputStream, java.lang.String)
	 */
	public String loadData(InputStream stream, String diachronicDatasetURI) throws Exception{
		
		return loadData(stream, diachronicDatasetURI, null);
		
	}

	public void loadMetadata(InputStream stream, String diachronicDatasetURI){
		
		Dataset dataset = DatasetFactory.createMem();
		RDFDataMgr.read(dataset, stream, null, Lang.JSONLD);					
		Iterator<String> names = dataset.listNames();
		Model jena = ModelFactory.createDefaultModel();
		while(names.hasNext()) {
			String name = (String) names.next();				    
			jena.add(dataset.getNamedModel(name));				   				    				    
		}
		jena.add(dataset.getDefaultModel());
		Model remoteModel = StoreConnection.getJenaModel(diachronicDatasetURI);
	    remoteModel.add(jena);
	    remoteModel.close();
		jena.close();
	    
	}
	
	/**
	 * Empties the given named graph from the store
	 * @param tempGraph the graph name to be emptied
	 */
	private void clearStageGraph(String tempGraph) {
		
		Connection conn = null;
		Statement stmt = null;
		try{			
			conn = StoreConnection.getConnection();											 
		    stmt = conn.createStatement ();
		    stmt.execute ("log_enable(3,1)");
		    stmt.executeQuery("SPARQL CLEAR GRAPH <"+tempGraph+">");
		    
		}catch(Exception e){
			logger.error(e.getMessage(), e);
			Graph graph = StoreConnection.getGraph(tempGraph);	    
			graph.clear();
			graph.close();
		}
		finally {		    
		    try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
		    try { if (conn != null) conn.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
		}
		
	}

	@SuppressWarnings("unused")
	private void createStageGraphStreaming(InputStream stream, String graphName) {
		
		Model remoteModel = StoreConnection.getJenaModel(graphName);
		
	    try {
	    	
			remoteModel.read(stream,null);
		} 
	    
	    catch (Exception e) {			
	    	
			logger.error(e.getMessage(), e);
		}
	    
	    remoteModel.close();
	}
	
	
	/**
	 * Loads the RDF data to the given named graph. 
	 * <p>
	 * The method uses the Virtuoso isql tools to load the data from a file saved in the filesystem. 
	 * So it saves the contents of the input stream to a file and passes the file name to the isql functions
	 * </p>
	 * @param stream	the input stream to read from
	 * @param graphName	the graph name to be created with the data from the input stream
	 * @param rdfFormat the RDF serialization format of the data contained in the steam
	 * @throws Exception 
	 */
	private void bulkLoadRDFDataToGraph(InputStream stream, String graphName, String rdfFormat) throws Exception {
		
		String fileExtension = determineFileExtension(rdfFormat);
		
		String fileName = "upload_file."+(new Date()).getTime()+fileExtension;
		Path path = Paths.get(StoreConnection.getBulkLoadPath()+fileName);
		Connection conn = null;
		Statement statement = null;
		try {
			Files.copy(stream, path);
			// do the ISQL stuff
			conn = StoreConnection.getConnection();		     
			statement = conn.createStatement();			
			String deletePastUploads = "delete from db.dba.load_list where ll_state='2'";
			statement.execute(deletePastUploads);
		    String bulkLoadSetQuery = "ld_dir('"+StoreConnection.getBulkLoadPath()+"', '"+fileName+"', '"+graphName+"')";		    
		    statement.execute(bulkLoadSetQuery);
		    String runBulkLoader = "rdf_loader_run()";
		    statement.execute(runBulkLoader);
		    //statement.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
		}
		finally {		    
		    try { if (statement != null) statement.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
		    try { if (conn != null) conn.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
		}
		
	}

	private String determineFileExtension(String rdfFormat) throws Exception {
		String fileExtension;
		if (rdfFormat == null)
			fileExtension = ".rdf"; //RDF/XML, the default
		else if ("RDF/XML".equals(rdfFormat))
			fileExtension = ".rdf";
		else if ("N-TRIPLE".equals(rdfFormat))
			fileExtension = ".nt";
		else if ("TURTLE".equals(rdfFormat))
			fileExtension = ".ttl";
		else 
			throw new Exception("Unknown RDF format: " + rdfFormat);
		return fileExtension;
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
	private static String splitDataset(String tempGraph, String diachronicDatasetURI, String versionNumber, boolean full) throws Exception{				
		
		Graph graph = StoreConnection.getGraph(tempGraph);	
		Model model = StoreConnection.getJenaModel(tempGraph);
		
		String query = "SELECT DISTINCT ?dataset FROM <"+tempGraph+"> WHERE {?dataset a <"+DiachronOntology.dataset+">}";
		//System.out.println(query);
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();
		String datasetURI = null;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			RDFNode dataset = rs.get("dataset");
			datasetURI = dataset.toString();
		}
		if(datasetURI == null) 
			throw new Exception("No dataset instantiation URI in input.");
		
		//String createdURI = diachronicDatasetURI;
		diachronicDatasetURI = validateDiachronicDatasetURI(tempGraph, diachronicDatasetURI);				
		//System.out.println(diachronicDatasetURI);
		ArrayList<RDFDataset> newDatasetVersions = selectDatasetMetadata(model, tempGraph, diachronicDatasetURI);
		/*for(RDFDataset d : newDatasetVersions)
			System.out.println("xxx " + d.getId());*/
		DictionaryService dict = StoreFactory.createDictionaryService();
		Graph dictGraph = StoreConnection.getGraph(RDFDictionary.getDictionaryNamedGraph());		
		if(versionNumber.equals(""))
			dict.addDatasetMetadata(dictGraph, newDatasetVersions, diachronicDatasetURI);
		else
			dict.addDatasetMetadata(dictGraph, newDatasetVersions, diachronicDatasetURI, versionNumber);
		//This will link the dataset version to its diachronic dataset, if this information exists in the stream.
							
		query = "SELECT DISTINCT ?rs ?ds FROM <"+tempGraph+"> WHERE {?ds <"+DiachronOntology.hasRecordSet+"> ?rs }";//. ?rs a <"+DiachronOntology.recordSet+">}";		
		vqe = QueryExecutionFactory.create (query, model);
		results = vqe.execSelect();
		
		while(results.hasNext()){
			QuerySolution rs = results.next();
			RDFNode recordSet = rs.get("rs");
			RDFNode dataset = rs.get("ds");
			if(full)
				insertRecordSetTriples(graph, recordSet.toString(), tempGraph);	
			//register recordset
			dict.addRecordSet(dictGraph, recordSet.toString(), dataset.toString());
		}
		vqe.close();	
		dictGraph.close();
		query = "SELECT DISTINCT ?ss FROM <"+tempGraph+"> WHERE {?ss a <"+DiachronOntology.schemaSet+">}";
		vqe = QueryExecutionFactory.create (query, model);
		results = vqe.execSelect();
		while(results.hasNext()){
			QuerySolution rs = results.next();
			RDFNode schemaSet = rs.get("ss");
			if(full)
				insertSchemaSetTriples(graph, schemaSet.toString(), tempGraph);
			dict.addSchemaSet(dictGraph, schemaSet.toString(), datasetURI);
		}
		vqe.close();		
		
		
						
		insertChangeSetTriples(model, tempGraph);		
		
		
		model.close();	
		graph.close();
		return datasetURI;
		
	}
	
	/**
	 * Inserts the triples associated with the dataset's record set.
	 * @param graph The connection VirtGraph to the archive.
	 * @param recordSet The URI of the record set to upload.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private static void insertRecordSetTriples(Graph graph, String recordSet, String tempGraph){
		//System.out.println(recordSet.toString());
		Graph graph1 = StoreConnection.getGraph(recordSet);
		GraphStore gs = GraphStoreFactory.create(graph1);
		gs.addGraph(NodeFactory.createURI(recordSet.toString()), graph1);
		String query = "INSERT { GRAPH <"+recordSet.toString()+"> {" +
							"<"+recordSet.toString()+"> ?p ?o" +
						"} } WHERE { GRAPH <"+tempGraph+">  " +
								"{<"+recordSet.toString()+"> ?p ?o FILTER(?p!=<"+DiachronOntology.hasRecord+">)}}";
		//System.out.println(query);
		VirtGraph virt = StoreConnection.getVirtGraph();
		VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, virt);
		vur.exec();
		
		/*UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute();*/
		//UpdateAction.parseExecute( query, graph);
		
		String queryRecords = "INSERT { GRAPH <"+recordSet.toString()+"> {?rec ?p ?o}} WHERE { GRAPH <"+tempGraph+"> {<"+recordSet.toString()+"> <"+DiachronOntology.hasRecord+"> ?rec . ?rec ?p ?o. }}";
		//UpdateAction.parseExecute( queryRecords, graph);
		vur = VirtuosoUpdateFactory.create(queryRecords, virt);
		vur.exec();
		
		/*queryObj = UpdateFactory.create(queryRecords); 
		qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); */
		
		String queryRecordAtts = "INSERT { GRAPH <"+recordSet.toString()+"> {?att ?p ?o} } WHERE { GRAPH <"+tempGraph+"> {<"+recordSet.toString()+"> <"+DiachronOntology.hasRecord+"> ?rec .?rec <"+DiachronOntology.hasRecordAttribute+"> ?att . ?att ?p ?o. }}";		
		//UpdateAction.parseExecute( queryRecordAtts, graph);
		vur = VirtuosoUpdateFactory.create(queryRecordAtts, virt);
		vur.exec();
		/*queryObj = UpdateFactory.create(queryRecordAtts); 
		qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); */
		virt.close();
		graph1.close();
	}
	
	/**
	 * Inserts the triples associated with the dataset's schema set.
	 * @param graph The connection VirtGraph to the archive.
	 * @param schemaSet The URI of the schema set to upload.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private static void insertSchemaSetTriples(Graph graph, String schemaSet, String tempGraph){
		
		String query = "INSERT INTO <"+schemaSet.toString()+"> {" +
							"<"+schemaSet.toString()+"> ?p ?o" +
						"} FROM <"+tempGraph+"> WHERE " +
								"{<"+schemaSet.toString()+"> ?p ?o}";
		UpdateAction.parseExecute( query, graph);
		
		query = "INSERT INTO <"+schemaSet.toString()+"> {" +
				"?o ?p2 ?o2" +
			"} FROM <"+tempGraph+"> WHERE " +
					"{<"+schemaSet.toString()+"> ?p ?o . ?o ?p2 ?o2}";
		UpdateAction.parseExecute( query, graph);
		
		query = "INSERT INTO <"+schemaSet.toString()+"> {" +
				"?o ?p3 ?o3" +
			"} FROM <"+tempGraph+"> WHERE " +
					"{<"+schemaSet.toString()+"> ?p1 [ ?p2 ?o ]. ?o ?p3 ?o3.}";
		UpdateAction.parseExecute( query, graph);
	}
	
	/**
	 * Inserts the triples associated with the change set defined in the input temporary graph.
	 * 
	 * @param graph The connection VirtGraph to the archive.	
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 */
	private static void insertChangeSetTriples(Model model, String tempGraph){
				
		String changeSetURI = "";
		String csURIQuery = "SELECT DISTINCT ?changeSet ?recordSet FROM <"+tempGraph+"> WHERE {" +
				"?changeSet a <"+DiachronOntology.changeSet+"> " +
				"OPTIONAL {?recordSet a <"+DiachronOntology.recordSet+">}" +
				"}";			
		QueryExecution vqe = QueryExecutionFactory.create (csURIQuery, model);
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
		if(existingRecordSet.equals("")) 
			versionsQuery += "?new }";
		else 
			versionsQuery += "<"+existingRecordSet+"> }";			

		vqe = QueryExecutionFactory.create (versionsQuery, model);
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

			QueryExecution vqeDataset = QueryExecutionFactory.create (datasetQuery, model);
			ResultSet resultsDataset = vqeDataset.execSelect();
			boolean changeSetAlreadyExists = true;
			while(resultsDataset.hasNext()){
				changeSetAlreadyExists = false;
				QuerySolution rsDataset = resultsDataset.next();
				String dictionaryQuery = "INSERT INTO <"+RDFDictionary.getDictionaryNamedGraph()+"> {<"+changeSetURI+"> a <"+DiachronOntology.changeSet+"> ; <"+DiachronOntology.oldVersion+"> <"+rsDataset.get("old_dataset").toString()+"> ; <"+DiachronOntology.newVersion+"> <"+rsDataset.get("new_dataset").toString()+"> }";
				QueryExecution vqeDatasetInsert = QueryExecutionFactory.create (dictionaryQuery, model);
				vqeDatasetInsert.execSelect();
			}
			vqeDataset.close();
			if(changeSetAlreadyExists) 
				return;
			
			String changesQuery = "INSERT INTO <"+changeSetURI+"> {?change ?p ?o }" +
					"WHERE {{SELECT ?change ?p ?o FROM <"+tempGraph+"> WHERE {?change <"+DiachronOntology.oldVersion+"> <"+rs.get("old").toString()+"> ; " + 
								   "<"+DiachronOntology.newVersion+"> <"+rs.get("new").toString()+"> ; " +
								   "?p ?o FILTER(?p!=<"+DiachronOntology.oldVersion+">n && ?p!=<"+DiachronOntology.newVersion+">) . " +
								   //"OPTIONAL {?o ?pp ?oo FILTER(?pp!=co:old_version}" +								   
					"}}}";
			QueryExecution vqeChanges = QueryExecutionFactory.create (changesQuery, model);
			vqeChanges.execSelect();
			String innerChangesQuery = "INSERT INTO <"+changeSetURI+"> {?inner ?p ?o}  "+
					" WHERE {" +
							"GRAPH <"+changeSetURI+"> {" +
										"?change ?prop ?inner " +
										"GRAPH <"+tempGraph+"> {?inner ?p ?o " +
										"FILTER NOT EXISTS {" +
											"?inner <"+DiachronOntology.oldVersion+"> []" +
										"}} . " +
									"}" +								
					"}";
			System.out.println(innerChangesQuery);
			Connection conn = null;											 
		    Statement stmt = null;
			try{
				//Class.forName("virtuoso.jdbc4.Driver");
				conn = StoreConnection.getConnection();											 
			    stmt = conn.createStatement ();
			    stmt.executeQuery ("SPARQL " + innerChangesQuery);
			} catch(Exception e){
				logger.error(e.getMessage(), e);
			}
			finally {		    
			    try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
			    try { if (conn != null) conn.close(); } catch (Exception e) {logger.error(e.getMessage(), e);};
			}
			/*VirtuosoQueryExecution vqeInnerChanges = VirtuosoQueryExecutionFactory.create (innerChangesQuery, graph);
			ResultSet resultsInnerChanges = vqeInnerChanges.execSelect();*/
			
		}vqe.close();				
	}
	
	/**
	 * Selects the metadata of the dataset.
	 * @param graph The connection's VirtGraph.
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 * @param diachronicDatasetURI The diachronic dataset URI provided by the user, if any. If it doesn't exist, it is looked for in the temporary graph.
	 */
	private static ArrayList<RDFDataset> selectDatasetMetadata(Model model, String tempGraph, String diachronicDatasetURI){
				
		//Calendar cal = GregorianCalendar.getInstance();
		//String timestamp = ResourceFactory.createTypedLiteral(cal).getString();
		/*ArrayList<RDFDataset> list = new ArrayList<RDFDataset>();
		String query = "SELECT ?dataset ?p ?o FROM <"+tempGraph+"> WHERE { ";
		query += "?dataset a <"+DiachronOntology.dataset+"> ; " +
							 "?p ?o} ORDER BY ?dataset";
		
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		String previousDatasetId = null;
		ArrayList<String[]> metadata = new ArrayList<String[]>();
		RDFDataset dataset = new RDFDataset();
		while(results.hasNext()){			
			QuerySolution rs = results.next();
			String datasetId = rs.get("dataset").toString();
			if(datasetId!=previousDatasetId){
				dataset.setMetadata(metadata);
				list.add(dataset);
				dataset = new RDFDataset();								
				dataset.setId(datasetId);								
				metadata = new ArrayList<String[]>();
			}
			metadata.add(new String[] {rs.get("p").toString(), rs.get("o").toString()});
			//dataset.se
			previousDatasetId = datasetId;
		}
		vqe.close();*/
		ArrayList<RDFDataset> list = new ArrayList<RDFDataset>();
		String query = "SELECT DISTINCT ?dataset FROM <"+tempGraph+"> WHERE { ";
		query += "?dataset a <"+DiachronOntology.dataset+"> } ";
		
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();				
		RDFDataset dataset = new RDFDataset();
		while(results.hasNext()){			
			QuerySolution rs = results.next();
			String datasetId = rs.get("dataset").toString();
			dataset = new RDFDataset();								
			dataset.setId(datasetId);
			
			String metaQuery = "SELECT ?p ?o FROM <"+tempGraph+"> WHERE {" +
					"<"+datasetId+"> ?p ?o }";
			QueryExecution metaVqe = QueryExecutionFactory.create (metaQuery, model);
			ResultSet metaResults = metaVqe.execSelect();
			ArrayList<String[]> metadata = new ArrayList<String[]>();
			while(metaResults.hasNext()){			
				QuerySolution metaRs = metaResults.next();				
				//metadata.add(new String[] {metaRs.get("p").toString(), metaRs.get("o").toString()});
				dataset.setMetaProperty(metaRs.get("p").toString(), metaRs.get("o").toString());
			}
			metaVqe.close();
			list.add(dataset);
		}
		vqe.close();		
		//model.close();
		return list;
		/*String query = "INSERT INTO <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{" +
					"?dataset ?p ?o ; <"+DiachronOntology.generatedAtTime+"> \""+timestamp+"\"^^xsd:dateTime " +
				"} " +
			"FROM <"+tempGraph+"> " +
				"WHERE {" +
					"?dataset a <"+DiachronOntology.dataset+"> ; " +
							 "?p ?o" +
						"}";	
		VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, graph);
		vur.exec();*/
		/*VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (query, graph);		
		vqe.execSelect();
		String queryDiachronic = "INSERT INTO <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{" +
					"<"+diachronicDatasetURI+"> ?p ?o" +
				"} " +
				"FROM <"+tempGraph+"> " +
				"WHERE {" +					
						 "<"+diachronicDatasetURI+"> ?p ?o" +
					   "}";
		VirtuosoQueryExecution vqeDiach = VirtuosoQueryExecutionFactory.create (queryDiachronic, graph);
		vqeDiach.execSelect();		*/
	}
	
	/**
	 * Validates the provided diachronic dataset URI to see if the dataset exists in the dictionary.
	 *
	 * @param tempGraph The URI of the temporary graph where the bulk loading has been performed.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to be validated in the dictionary.
	 * @return A string containing the fixed URI of the diachronic dataset.
	 */
	private static String validateDiachronicDatasetURI(String tempGraph, String diachronicDatasetURI){
		
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
		
		Model model = StoreConnection.getJenaModel(tempGraph);						
		QueryExecution vqe = QueryExecutionFactory.create (diachronicQuery, model);
		ResultSet results = vqe.execSelect();
		while(results.hasNext()){
			QuerySolution rs = results.next();
			String existingDiachronicDatasetURI = rs.get("diachronicDataset").toString();
			if(!existingDiachronicDatasetURI.equals(diachronicDatasetURI)) 
			diachronicDatasetURI = existingDiachronicDatasetURI;
		}
		vqe.close();
		model.close();
		return diachronicDatasetURI;
	}
	
	public static void main (String args[]) {
		
		try {
			@SuppressWarnings("unused")
			String datasetURI = splitDataset("http://www.diachron-fp7.eu/resource/stageGraph/1417516246097", "http://www.diachron-fp7.eu/resource/diachronicDataset/4e58d4", "", true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
