package org.athena.imis.diachron.archive.core.dataloader;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;
import org.athena.imis.diachron.archive.api.QueryStatement;
import org.athena.imis.diachron.archive.api.StatementFactory;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.ModelsFactory;
import org.athena.imis.diachron.archive.models.RDFDataset;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtModel;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

/**
 * 
 * Provides a bridge between the dictionary of datasets in the archive and the archive's other functionalities.
 *
 */
public class RDFDictionary implements DictionaryService {
	
	static final String dictionaryNamedGraph = "XAXA";

	/**
	 * Fetches the named graph where the dictionary is defined in the archive.
	 * @return A String containing a URI of the dictionary graph.
	 */
	public static String getDictionaryNamedGraph() {
		return dictionaryNamedGraph;
	}

    private final VirtuosoDataSource dataSource;

	public RDFDictionary(VirtuosoDataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	/**
	 * Creates a diachronic dataset object in the dictionary of datasets.
	 * 
	 * FIXME the lack of a DataStore interface makes this functionality Virtuoso-only
	 * 
	 * @param dds The DiachronicDataset to be created.
	 * @return A String URI of the created diachronic dataset.
	 *  
	 */
	@Override
	public String createDiachronicDataset(DiachronicDataset dds) {

		String URI = createDiachronicDatasetId();
				
		VirtGraph graph = new VirtGraph(dictionaryNamedGraph, dataSource);
		Model model = new VirtModel(graph);
		try {
		  
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
		return URI;
		} finally {
		  model.close();
		  graph.close();
		}

	}
	
	/**
	 * Create a URI for a new diachronic dataset.
	 * @return A string URI.
	 */
	public String createDiachronicDatasetId() {
		// TODO proper URI creation
		// sequential or random but at least check for conflict with existing 
		double multi = (double)(10^8);
		int id = (int)(multi*Math.random());
		
		String URI = "http://www.diachron-fp7.eu/resource/diachronicDataset/"+Integer.toHexString(id).toLowerCase();
		return URI;
	}

	/**
	 * Fetches a list of diachronic datasets, as DiachronicDataset objects, existing in the DIACHRON archive.
	 * @return A list of diachronic datasets existing in the archive.
	 */
	@Override
	public List<DiachronicDataset> getListOfDiachronicDatasets() {
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
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
	@Override
	public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDataset) {		
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
		Query q = new Query();
		q.setQueryText("SELECT DISTINCT ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  <"+diachronicDataset.getId()+"> <"+DiachronOntology.hasInstantiation+"> ?o . " +
						"?o <"+DiachronOntology.generatedAtTime+"> ?time} ORDER BY DESC(?time) ");
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
			datasets.add(ds);			
		}				
		return datasets;	
	}
	
	/**
	 * {@inheritDoc}
	 */
	public Hashtable<String, Object>  getDiachronicDatasetMetadata(String diachronicDatasetId) {
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
		Query q = new Query();
		q.setQueryText("SELECT ?p ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +
				"> WHERE {  <"+diachronicDatasetId+"> ?p ?o " +
						" FILTER (?p!= <"+DiachronOntology.hasInstantiation+">) } " +
						"");
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
	@Override
	public DiachronicDataset getDiachronicDataset(String id) {
		DiachronicDataset dds =  ModelsFactory.createDiachronicDataset();
		dds.setId(id);		
		dds.setMetaProperties(getDiachronicDatasetMetadata(id));
		for (Dataset dataset: getListOfDatasets(dds))
			dds.addDatasetInstatiation(dataset);
		return dds;
	}

}
