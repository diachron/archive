package org.athena.imis.diachron.archive.web.services;

import java.net.URL;
import java.util.Date;
import java.util.List;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;
import org.athena.imis.diachron.archive.api.Query;
import org.athena.imis.diachron.archive.api.QueryStatement;
import org.athena.imis.diachron.archive.api.StatementFactory;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.core.dataloader.RDFDictionary;
import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.ModelsFactory;
import org.athena.imis.diachron.archive.models.Serializer;

import virtuoso.jdbc4.VirtuosoDataSource;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * 
 * This class contains query templates that are commonly used in the archive.
 *
 */
public final class QueryUtil {

  private final VirtuosoDataSource dataSource;
  
  public QueryUtil(VirtuosoDataSource dataSource) {
    this.dataSource = dataSource;
  }
	
	/**
	 * Lists the diachronic datasets that exist in the DIACHRON archive.
	 * 
	 * @return A String containing SPARQL results as RDF/JSON of the existing diachronic datasets.  
	 * @throws Exception
	 */
	public String listDiachronicDatasets() throws Exception {
		Serializer ser = ModelsFactory.getSerializer();
		DictionaryService dict = StoreFactory.createPersDictionaryService(dataSource);
		
		List<DiachronicDataset> list = dict.getListOfDiachronicDatasets();
        return ser.serialize(list);
	}
		
	/**
	 * Fetches the whole dataset, de-reified (i.e. in its original, non-DIACHRON form) as RDF/JSON.
	 *  
	 * @param datasetId	The URI of the dataset to be fetched.
	 * @return A RDF/JSON string of the de-reified dataset.
	 */
	public String getDatasetVersionById(String datasetId) {
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
		String queryString = "CONSTRUCT {?s ?p ?o} " +
								"WHERE {{" +
								"SELECT ?s ?p ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() +"> " +
										"WHERE " +
										"{ <"+datasetId+"> <"+DiachronOntology.hasRecordSet+"> ?rs ." +
								"GRAPH ?rs {" +
									"{?rec <"+DiachronOntology.subject+"> ?s ; " +
									"<"+DiachronOntology.hasRecordAttribute+"> [<"+DiachronOntology.predicate+"> ?p ; <"+DiachronOntology.object+"> ?o]}" +
									"UNION {?s a <"+DiachronOntology.multidimensionalObservation+"> ; <"+DiachronOntology.hasRecordAttribute+"> [<"+DiachronOntology.predicate+"> ?p ; <"+DiachronOntology.object+"> ?o]}" +
										"}" +
									"}" +
								"}"+ 
								"}";		
		Query q = new Query();
		q.setQueryText(queryString);
		q.setQueryType("CONSTRUCT");
		ArchiveResultSet res = query.executeQuery(q);		
		
		return res.serializeResults("CONSTRUCT");
	}
	
	/**
	 * Lists the instantiations (versions) of the diachronic dataset defined in the input parameters.
	 * 
	 * @param diachronicDatasetId The diachronic dataset URI of which the versions will be listed.
	 * @param time A list of temporal conditions that will limit the resulting list of versions.
	 * @return A RDF/JSON String of SPARQL results containing the list of versions of the input diachronic dataset. 
	 * @throws Exception
	 */
	public String listDatasetVersions(String diachronicDatasetId, List<TimeConditions> time) throws Exception {
		Serializer ser = ModelsFactory.getSerializer();
		DictionaryService dict = StoreFactory.createPersDictionaryService(dataSource);
		
		List<Dataset> list = dict.getListOfDatasets(dict.getDiachronicDataset(diachronicDatasetId));		
        return ser.serialize(list);
	}
	
	/**
	 * Fetches the last dataset version of the diachronic dataset defined in the input parameter.
	 * @param diachronicDatasetId The URI of the diachronic dataset whose last version is to be fetched.
	 * @return A RDF/JSON String of the last version (de-reified) of the defined diachronic dataset. 
	 */
	public String getLastDatasetVersion(String diachronicDatasetId) {
		DictionaryService dict = StoreFactory.createPersDictionaryService(dataSource);
		Dataset ds = dict.getDiachronicDataset(diachronicDatasetId).getDatasetInstatiations().get(0);
		
		return getDatasetVersionById(ds.getId());
		
	}
	
	/**
	 * Fetches the change set between two versions defined in the input parameters.
	 * @param old_version The URI of the older version of the change set.
	 * @param new_version The URI of the newer version of the change set.
	 * @return The URI of the change set between old_version and new_version.
	 */
	public String getChangeSet(String old_version, String new_version){
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
		String queryString = "SELECT ?changeSet " +
								"FROM <" + RDFDictionary.getDictionaryNamedGraph() +
								"> WHERE { ?changeSet a <"+DiachronOntology.changeSet+"> ; " +
											" <"+DiachronOntology.oldVersion+"> <"+old_version+"> ;" +
											" <"+DiachronOntology.newVersion+"> <"+new_version+"> " +			
								"}";
		Query q = new Query();
		q.setQueryText(queryString);
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		ResultSet results = res.getJenaResultSet();
		String returnString = null;
		if(results.hasNext()){
			QuerySolution rs = results.next();
			returnString = rs.get("changeSet").toString();
		}
		return returnString;
	}
	
	/**
	 * Fetches a list of changes from a change set defined in the input parameters. The changes can be limited 
	 * to match a particular change type, and further limited to changes applied on specific parameters.
	 * 
	 * @param changeSet The URI of the change set whose changes are to be fetched. 
	 * @param changeType The change type URI (if any) to limit the returned changes. 
	 * @param parameters A list of String arrays containing the parameter name and parameter value of the change. 
	 * @return RDF/JSON SPARQL results of the changes list.
	 */
	public String getChangesFromChangeSet(String changeSet, String changeType, List<String[]> parameters){
		QueryStatement query = StatementFactory.createQueryStatement(dataSource);
		String queryString = "SELECT ?change ?p ?o FROM <"+changeSet+"> WHERE {";
		if(!changeType.equals("")){
			queryString+="?change a <"+changeType+"> . ";			
			for(String[] param : parameters){
				String param1 = param[1];;
				try {
			        param1 = "<"+new URL(param1)+">";
			    } catch (Exception e1) {
			        param1 = "\""+param1+"\"";
			    }
				queryString+="?change <"+param[0]+"> [<"+DiachronOntology.paramValue+"> "+param1+"] . "; 
			}			
		}
		queryString+="?change ?p ?o . }";
		System.out.println(queryString);
		Query q = new Query();
		q.setQueryText(queryString);
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		String resultString = res.serializeResults(q.getQueryType());
				
		return resultString;
	}
	
}

/**
 * 
 * Instances of this class represent time conditions that can limit temporal queries.
 *
 */
class TimeConditions {
	public static enum OPERATORS {
		EQUAL, BEFORE, AFTER 
	}
	private static enum JOINS {
		AND, OR
	}
	
	private Date value;
	private OPERATORS operator;
	
}
