package org.athena.imis.diachron.archive.core.dataloader;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.List;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;
import org.athena.imis.diachron.archive.api.QueryStatement;
import org.athena.imis.diachron.archive.api.StatementFactory;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronURIFactory;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.ModelsFactory;
import org.athena.imis.diachron.archive.models.RDFDataset;
import org.athena.imis.diachron.archive.models.RDFRecordSet;
import org.athena.imis.diachron.archive.models.RDFSchemaSet;
import org.athena.imis.diachron.archive.models.RecordSet;
import org.athena.imis.diachron.archive.models.SchemaSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.update.GraphStore;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * 
 * Provides a bridge between the dictionary of datasets in the archive and the archive's other functionalities.
 *
 */
public class RDFDictionary implements DictionaryService {
	
	static final String dictionaryNamedGraph = "http://www.diachron-fp7.eu/archive/dictionary";
	private static final Logger logger = LoggerFactory.getLogger(RDFDictionary.class);
	/**
	 * Fetches the named graph where the dictionary is defined in the archive.
	 * @return A String containing a URI of the dictionary graph.
	 */
	public static String getDictionaryNamedGraph() {
		return dictionaryNamedGraph;
	}

	// just to change the visibility
	RDFDictionary() {
		
	}
	
	/**
	 * Creates a diachronic dataset object in the dictionary of datasets.
	 * 
	 * @param dds The DiachronicDataset to be created.
	 * @return A String URI of the created diachronic dataset.
	 * @throws Exception 
	 *  
	 */
	public String createDiachronicDataset(DiachronicDataset dds, String datasetName) throws Exception {

		String URI = createDiachronicDatasetId(datasetName);
				
		Model model = StoreConnection.getJenaModel(dictionaryNamedGraph);
		Resource diachronicDatasetResource = model.createResource(URI, DiachronOntology.diachronicDataset);
		
		for(String predicate : dds.getMetaPropertiesNames()){
			String objectString = (String) dds.getMetaProperty(predicate);
			try{
				URL object = new URL(objectString);				
				diachronicDatasetResource.addProperty(ResourceFactory.createProperty(predicate), model.createResource(object.toString()));
			} catch(MalformedURLException e){
				diachronicDatasetResource.addProperty(ResourceFactory.createProperty(predicate), model.createLiteral(objectString));
			}
		}
		model.close();
		return URI;

	}
	
	/**
	 * Create a URI for a new diachronic dataset.
	 * @return A string URI.
	 */
	public String createDiachronicDatasetId(String datasetName) {
		DiachronURIFactory uriFactory = new DiachronURIFactory(datasetName, "");
		return uriFactory.generateDiachronicDatasetUri().toString();
	}

	/**
	 * Fetches a list of diachronic datasets, as DiachronicDataset objects, existing in the DIACHRON archive.
	 * @return A list of diachronic datasets existing in the archive.
	 */
	public List<DiachronicDataset> getListOfDiachronicDatasets() {
		QueryStatement query = StatementFactory.createQueryStatement();
		Query q = new Query();
		q.setQueryText("SELECT DISTINCT ?s FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  ?s a <"+DiachronOntology.diachronicDataset+">} ");
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		
		List<DiachronicDataset> diachronicDatasets = new ArrayList<DiachronicDataset>(); 
		ResultSet rs = res.getJenaResultSet();
		while (rs.hasNext()) {
			QuerySolution qs = rs.next();
			String diachronicDatasetId = qs.get("s").toString();
			DiachronicDataset dds = ModelsFactory.createDiachronicDataset();
			dds.setId(diachronicDatasetId);
			diachronicDatasets.add(dds);
		}		
		return diachronicDatasets;
	}

	/**
	 * {@inheritDoc}
	 */
	public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDataset) {
		
		QueryStatement query = StatementFactory.createQueryStatement();
		Query q = new Query();
		q.setQueryText("SELECT DISTINCT ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  <"+diachronicDataset.getId()+"> <"+DiachronOntology.hasInstantiation+"> ?o . " +
						"OPTIONAL { "
						+ "{?o <"+DiachronOntology.generatedAtTime+"> ?time} "
								+ "UNION {?o <"+DCTerms.created.getURI().toString()+"> ?time}"
								+ "}"
								+ "} ORDER BY DESC(?time) ");
		q.setQueryType("SELECT");		
		ArchiveResultSet res = query.executeQuery(q);
		List<Dataset> datasets = new ArrayList<Dataset>();
		ResultSet rs = res.getJenaResultSet();
		while (rs.hasNext()) {
			QuerySolution qs = rs.next();
			String datasetId = qs.get("o").toString();
			
			Dataset ds = new RDFDataset();			
			ds.setId(datasetId);			
			ds.setDiachronicDataset(diachronicDataset);
			ds.setMetaProperties(getDatasetMetadata(datasetId));
			
			Query qin = new Query();
			qin.setQueryText(" SELECT ?fm ?rs ?added ?deleted FROM <"+RDFDictionary.getDictionaryNamedGraph()+">"
					+ " WHERE {"
					+ "<"+datasetId+"> <"+DiachronOntology.isFullyMaterialized+"> ?fm ; "
							+ "<"+DiachronOntology.hasRecordSet+"> ?rs . "
							+ "OPTIONAL { <"+datasetId+"> <"+DiachronOntology.addedGraph+"> ?added ; <"+DiachronOntology.deletedGraph+"> ?deleted }"
							+ "OPTIONAL { <"+datasetId+"> <"+DiachronOntology.hasSchemaSet+"> ?ss  }"
							+ "}"
							);
			qin.setQueryType("SELECT");
			
			QueryStatement queryin = StatementFactory.createQueryStatement();
			ArchiveResultSet resin = queryin.executeQuery(qin);					
			ResultSet rsin = resin.getJenaResultSet();
			while (rsin.hasNext()) {			
				
				QuerySolution qsin = rsin.next();				
				String isFully = qsin.getLiteral("fm").getString();
				if(isFully.equals("false"))
					ds.setFullyMaterialized(false);
				else
					ds.setFullyMaterialized(true);
				if(qsin.contains("added") && qsin.contains("deleted"))
					ds.setDeltaGraphs(qsin.get("added").toString(), qsin.get("deleted").toString());
				
				RecordSet rset = new RDFRecordSet();
				rset.setId(qsin.get("rs").toString());
				ds.setRecordSet(rset);
				
				if(qsin.contains("ss")){
					SchemaSet sset = new RDFSchemaSet();
					sset.setId(qsin.get("ss").toString());
					ds.setSchemaSet(sset);
				}
				if(!ds.isFullyMaterialized()){
					
					String qfm = ""
							+ "SELECT DISTINCT ?version ?fm ?cs FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> WHERE {"
									+ "?diachronic <"+DiachronOntology.hasInstantiation+"> <"+datasetId+"> ; "
											+ " <"+DiachronOntology.hasInstantiation+"> ?version . "
									+ " ?version a <"+DiachronOntology.dataset+"> ;"
									+ " <"+DiachronOntology.isFullyMaterialized+"> ?fm ; "
									+ " <"+DiachronOntology.creationTime+"> ?time . "
									+ " <"+datasetId+"> <"+DiachronOntology.creationTime+"> ?time2 . "
									//+ " FILTER(<"+XSD.dateTime+">(?time2) > <"+XSD.dateTime+">(?time))"
									+ " FILTER(?time2 > ?time)"
									+ " FILTER(?version!=<"+datasetId+">) "
									+ " OPTIONAL {?cs <"+DiachronOntology.oldVersion+"> ?version }"
									+ "} ORDER BY DESC(?time)";					
					
					qin.setQueryText(qfm);
					
					queryin.executeQuery(qin);
					
					ArchiveResultSet resin_fm = queryin.executeQuery(qin);	
					
					ResultSet rsin_fm = resin_fm.getJenaResultSet();
					
					ArrayList<String> changeSets = new ArrayList<String>();
					
					while (rsin_fm.hasNext()) {								
						
						QuerySolution qsin_fm = rsin_fm.next();
						
						//if(qsin_fm.contains("cs"))
						changeSets.add(qsin_fm.get("cs").toString());
						
						if(qsin_fm.getLiteral("fm").getBoolean()){
							
							ds.setLastFullyMaterialized(qsin_fm.get("version").toString());
							ds.setListOfChangesets(changeSets);
							break;
						}
						
					}
					
					
					
				}
				
				String qfm = ""
						+ "SELECT DISTINCT ?cs_old ?cs_new FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> WHERE {"
								
								+ "OPTIONAL{ <"+datasetId+"> <"+DiachronOntology.hasChangeSet+"> ?cs_old ."
										+ "?cs_old <"+DiachronOntology.oldVersion+"> <"+datasetId+"> } ."
								+ "OPTIONAL{ <"+datasetId+"> <"+DiachronOntology.hasChangeSet+"> ?cs_new ."
										+ "?cs_new <"+DiachronOntology.newVersion+"> <"+datasetId+"> } ."								
								+ "}";					
				
				qin.setQueryText(qfm);
				
				queryin.executeQuery(qin);
				
				ArchiveResultSet resin_fm = queryin.executeQuery(qin);	
				
				ResultSet rsin_fm = resin_fm.getJenaResultSet();								
				
				while (rsin_fm.hasNext()) {								
					
					QuerySolution qsin_fm = rsin_fm.next();
					if(qsin_fm.contains("cs_old")) ds.setChangeSetOld(qsin_fm.get("cs_old").toString());
					
					if(qsin_fm.contains("cs_new")) ds.setChangeSetNew(qsin_fm.get("cs_new").toString());
					
				}
				
				
				
			}
			
			
			datasets.add(ds);			
		}				
		return datasets;	
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Hashtable<String, Object>  getDiachronicDatasetMetadata(String diachronicDatasetId) {
		QueryStatement query = StatementFactory.createQueryStatement();
		Query q = new Query();
		q.setQueryText("SELECT ?p ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  <"+diachronicDatasetId+"> ?p ?o " +
						" FILTER (?p!= <"+DiachronOntology.hasInstantiation+">) } ");
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		
		Hashtable<String, Object> metaProperties = new Hashtable<String, Object>();
			
		ResultSet rs = res.getJenaResultSet();
		while (rs.hasNext()) {
			QuerySolution qs = rs.next();
			String value = qs.get("o").toString();
			String name = qs.get("p").toString();
			metaProperties.put(name, value);
			
		}
		return metaProperties;	
	}
	
	/**
	 * Returns the metadata associated with the specified dataset version.
	 * @param datasetId The dataset URI.
	 * @return	A Hashtable of predicate-object pairs.
	 */
	private Hashtable<String, Object>  getDatasetMetadata(String datasetId) {
		QueryStatement query = StatementFactory.createQueryStatement();
		Query q = new Query();
		q.setQueryText("SELECT ?p ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  <"+datasetId+"> ?p ?o } ");
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		
		Hashtable<String, Object> metaProperties = new Hashtable<String, Object>();
			
		ResultSet rs = res.getJenaResultSet();
		while (rs.hasNext()) {
			QuerySolution qs = rs.next();
			String value = qs.get("o").toString();
			String name = qs.get("p").toString();
			metaProperties.put(name, value);
			
		}
		return metaProperties;	
	}
	
	
	
	/**
	 * {@inheritDoc}
	 */
	public DiachronicDataset getDiachronicDataset(String id) {
		DiachronicDataset dds =  ModelsFactory.createDiachronicDataset();
		dds.setId(id);		
		dds.setMetaProperties(getDiachronicDatasetMetadata(id));
		for (Dataset dataset: getListOfDatasets(dds))
			dds.addDatasetInstatiation(dataset);
		return dds;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Dataset getDataset(String id) {		
		QueryStatement query = StatementFactory.createQueryStatement();
		Query q = new Query();
		q.setQueryText("SELECT DISTINCT ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE { [] <"+DiachronOntology.hasInstantiation+"> <"+id+"> ");
		q.setQueryType("SELECT");		
		ArchiveResultSet res = query.executeQuery(q);		
		ResultSet rs = res.getJenaResultSet();
		if (rs.hasNext()) {			
			Dataset ds = new RDFDataset();			
			ds.setId(id);
			return ds;
		}				
		return null;	
	}

	/**
	 * Adds a new dataset version to the peristent storage of the archive.
	 * @param graph The connection to the archive.
	 * @param diachronicDatasetURI The diachronic dataset to be updated.
	 * @param datasetURI The URI of the new dataset version.
	 * @param fullyMaterialized A boolean indicator of whether the dataset should be stored fully (true) or just the change set (false).
	 */
	@Override
	public void addDataset(Graph graph, String diachronicDatasetURI, String datasetURI, boolean fullyMaterialized) {
		Calendar cal = GregorianCalendar.getInstance();
		String timestamp = ResourceFactory.createTypedLiteral(cal).getString();		
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{ <"+diachronicDatasetURI+"> <"+DiachronOntology.hasInstantiation+"> <"+datasetURI+"> ."			
				+ "<"+datasetURI+">  <"+DiachronOntology.creationTime.getURI()+"> \""+timestamp+"\" ; "
				+ " <"+DiachronOntology.isFullyMaterialized+"> \""+fullyMaterialized+"\" "				
				+ "}}";		
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
		
	}
	
	/**
	 * Adds a new record set to the persistent storage of the archive.
	 * @param graph The connection to the archive.
	 * @param recordSetURI The URI of the record set to be added.
	 * @param datasetId The URI of the dataset where the new record set belongs.
	 */
	@Override
	public void addRecordSet(Graph graph, String recordSetURI, String datasetId) {
		//TODO refactor to remove the Graph input param
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{ <"+datasetId+"> <"+DiachronOntology.hasRecordSet+"> <"+recordSetURI+"> . <"+recordSetURI+"> <"+RDF.type+"> <"+DiachronOntology.recordSet+">}}";
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
	}
	
	/**
	 * Adds a new schema set to the persistent storage of the archive.
	 * @param graph The connection to the archive.
	 * @param schemaSetURI The URI of the schema set to be added.
	 * @param datasetId The URI of the dataset where the new schema set belongs.
	 */
	@Override
	public void addSchemaSet(Graph graph, String schemaSetURI, String datasetId) {
		//TODO refactor to remove the Graph input param
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{ <"+datasetId+"> <"+DiachronOntology.hasSchemaSet+"> <"+schemaSetURI+"> }}";
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
	}
	
	/**
	 * Adds the dictionary metadata for a list of dataset versions of a specified diachronic dataset.
	 * @param graph The connection to the archive.
	 * @param list A list of dataset versions to be associated with the diachronic dataset.
	 * @param diachronicDatasetURI The URI of the diachronic dataset.
	 * @param versionNumber An optional version number to be associated with the dataset versions.
	 */
	public void addDatasetMetadata(Graph graph, ArrayList<RDFDataset> list, 
			String diachronicDatasetURI, 
			String versionNumber, 
			boolean fullyMaterialized
			){
			
		
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		Calendar cal = GregorianCalendar.getInstance();
		String timestamp = ResourceFactory.createTypedLiteral(cal).getString();
		String query ;		
		
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj ;//= UpdateFactory.create(query); 
		UpdateProcessor qexec ;//= UpdateExecutionFactory.create(queryObj,gs); 
			
				
		for(RDFDataset dataset : list){
			
			query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
					"{ <"+diachronicDatasetURI+"> <"+DiachronOntology.hasInstantiation+"> <"+dataset.getId()+"> ."			
					+ "<"+dataset.getId()+">  <"+DiachronOntology.creationTime.getURI()+"> \""+timestamp+"\" ; "
					+ " <"+DiachronOntology.isFullyMaterialized+"> \""+fullyMaterialized+"\" "				
					+ "}}";		
			
			queryObj = UpdateFactory.create(query); 
			qexec = UpdateExecutionFactory.create(queryObj,gs); 
			qexec.execute(); 			
			
			query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> {" ;					
			query += "<"+dataset.getId()+"> <"+DCTerms.created.getURI().toString()+"> \""+timestamp+"\" .";
			query += " } }";			
			
			queryObj = UpdateFactory.create(query); 
			qexec = UpdateExecutionFactory.create(queryObj,gs); 
			qexec.execute(); 
			
			if(!versionNumber.equals("")){
				query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> {" ;
				query += "<"+dataset.getId()+"> <"+DCTerms.hasVersion.getURI().toString()+"> \""+versionNumber+"\" .";
				query += " } }";				
				queryObj = UpdateFactory.create(query); 
				qexec = UpdateExecutionFactory.create(queryObj,gs); 
				qexec.execute(); 				
			}
			
			
			for(String propertyName : dataset.getMetaPropertiesNames()){
				String object = (String) dataset.getMetaProperty(propertyName);
				query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> {" ;
				query += "<"+dataset.getId()+"> <"+propertyName+"> ";
				try {
					object = "<"+new URL(object)+">";
			    } catch (Exception e1) {
			    	object = "\""+object+"\"";
			    }
				query += object + " . ";
				query += " } }";
				queryObj = UpdateFactory.create(query); 
				qexec = UpdateExecutionFactory.create(queryObj,gs); 
				qexec.execute();
			}	
			/*dataset.setMetaProperty(DCTerms.created.getURI().toString(), timestamp);
			if(!versionNumber.equals("")){								
				dataset.setMetaProperty(DCTerms.hasVersion.getURI().toString(), versionNumber);
			}*/
			dataset.setMetaProperties(getDatasetMetadata(dataset.getId()));
			
		}
		
		
	}
	
	/**
	 * Adds the dictionary metadata for a list of dataset versions of a specified diachronic dataset.
	 * @param graph The connection to the archive.
	 * @param list A list of dataset versions to be associated with the diachronic dataset.
	 * @param diachronicDatasetURI The URI of the diachronic dataset.	
	 */
	public void addDatasetMetadata(Graph graph, ArrayList<RDFDataset> list, String diachronicDatasetURI){
		
		Calendar cal = GregorianCalendar.getInstance();
		String timestamp = ResourceFactory.createTypedLiteral(cal).getString();
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{";
		for(RDFDataset dataset : list){
			query += "<"+dataset.getId()+"> <"+DCTerms.created.getURI().toString()+"> \""+timestamp+"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .";
			dataset.setMetaProperty(DCTerms.created.getURI().toString(), timestamp);					
			
			for(String propertyName : dataset.getMetaPropertiesNames()){
				String object = (String) dataset.getMetaProperty(propertyName);
				query += "<"+dataset.getId()+"> <"+propertyName+"> ";
				try {
					object = "<"+new URL(object)+">";
			    } catch (Exception e1) {
			    	object = "\""+object+"\"";
			    }
				query += object + " . ";
			}
			dataset.setMetaProperties(getDatasetMetadata(dataset.getId()));
		}
		query += " } }";						
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
		
	
		
	}
	
	
	
}
