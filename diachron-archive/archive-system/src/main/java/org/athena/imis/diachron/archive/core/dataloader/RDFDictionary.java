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

/**
 * 
 * Provides a bridge between the dictionary of datasets in the archive and the archive's other functionalities.
 *
 */
public class RDFDictionary implements DictionaryService {
	
	static final String dictionaryNamedGraph = "http://www.diachron-fp7.eu/archive/dictionary";

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
						"OPTIONAL { {?o <"+DiachronOntology.generatedAtTime+"> ?time} "
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
			//Dataset ds = ModelsFactory.createDataset(diachronicDataset);
			Dataset ds = new RDFDataset();			
			ds.setId(datasetId);
			
			ds.setMetaProperties(getDatasetMetadata(datasetId));
			
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

	@Override
	public void addDataset(Graph graph, String diachronicDatasetURI, String datasetURI) {
		Calendar cal = GregorianCalendar.getInstance();
		String timestamp = ResourceFactory.createTypedLiteral(cal).getString();		
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{ <"+diachronicDatasetURI+"> <"+DiachronOntology.hasInstantiation+"> <"+datasetURI+"> ."
				//+ "<"+datasetURI+">  <"+DiachronOntology.generatedAtTime+"> \""+timestamp+"\" " 
				+ "<"+datasetURI+">  <"+DCTerms.created.getURI()+"> \""+timestamp+"\" "
				+ "}}";
		//System.out.println(query);
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
		
	}
	
	@Override
	public void addRecordSet(Graph graph, String recordSetURI, String datasetId) {
		//TODO refactor to remove the Graph input param
		String query = "INSERT DATA { GRAPH <"+RDFDictionary.dictionaryNamedGraph+"> " +
				"{ <"+datasetId+"> <"+DiachronOntology.hasRecordSet+"> <"+recordSetURI+"> }}";
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
	}
	
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
		}
		query += " } }";						
		GraphStore gs = GraphStoreFactory.create(graph);
		gs.addGraph(NodeFactory.createURI(RDFDictionary.getDictionaryNamedGraph()), graph);
		UpdateRequest queryObj = UpdateFactory.create(query); 
		UpdateProcessor qexec = UpdateExecutionFactory.create(queryObj,gs); 
		qexec.execute(); 
		//UpdateAction.parseExecute( query, graph);
		
		//VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(query, graph);
		//vur.exec();
		
	}
	
}
