package org.athena.imis.diachron.archive.api;

import java.net.URL;
import java.util.Date;
import java.util.List;

import org.athena.imis.diachron.archive.api.ArchiveResultSet.SerializationFormat;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.core.dataloader.RDFDictionary;
import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.ModelsFactory;
import org.athena.imis.diachron.archive.models.Serializer;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

/**
 * 
 * This class contains query templates that are commonly used in the archive.
 *
 */
public class QueryLib {
	
	private SerializationFormat serializationFormat = null;
	
	/**
	 * Lists the diachronic datasets that exist in the DIACHRON archive.
	 * 
	 * @return A String containing SPARQL results as RDF/JSON of the existing diachronic datasets.  
	 * @throws Exception
	 */
	public String listDiachronicDatasets() throws Exception {
		Serializer ser = ModelsFactory.getSerializer();
		DictionaryService dict = StoreFactory.createDictionaryService();
		
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
		QueryStatement query = StatementFactory.createQueryStatement();
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
		if (getSerializationFormat() != null)
			res.setSerializationFormat(getSerializationFormat());
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
	public String listDatasetVersions(String diachronicDatasetId, List<Condition> time) throws Exception {
		//TODO aligh with result set serialization
		Serializer ser = ModelsFactory.getSerializer();
		DictionaryService dict = StoreFactory.createDictionaryService();
		DiachronicDataset diachronicDataset = dict.getDiachronicDataset(diachronicDatasetId);
		if (diachronicDataset != null) {		
			List<Dataset> list = dict.getListOfDatasets(diachronicDataset);
			return ser.serialize(list);
		} else {
			return null;
		}
			
        
	}
	
	/**
	 * Fetches the last dataset version of the diachronic dataset defined in the input parameter.
	 * @param diachronicDatasetId The URI of the diachronic dataset whose last version is to be fetched.
	 * @return A RDF/JSON String of the last version (de-reified) of the defined diachronic dataset. 
	 */
	public String getLastDatasetVersion(String diachronicDatasetId) {
		DictionaryService dict = StoreFactory.createDictionaryService();
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
		QueryStatement query = StatementFactory.createQueryStatement();
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
		QueryStatement query = StatementFactory.createQueryStatement();
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
		if (getSerializationFormat() != null)
			res.setSerializationFormat(getSerializationFormat());
		
		String resultString = res.serializeResults(q.getQueryType());
		return resultString;
	}
	
	public String getDiachronicDatasetMetadata(String diachronicDatasetId) throws Exception {
		//check for existance of the Diachronic Dataset
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(diachronicDatasetId) == null)
			throw new Exception("Non-existing Diachronic Dataset");
		else {
			QueryStatement query = StatementFactory.createQueryStatement();
			String queryString = "CONSTRUCT {?s ?p ?o} " +
						"WHERE {{" +
						"SELECT ?s ?p ?o FROM <" + diachronicDatasetId +"> WHERE {?s ?p ?o}}}";
			Query q = new Query();
			q.setQueryText(queryString);
			q.setQueryType("CONSTRUCT");
			ArchiveResultSet res = query.executeQuery(q);		
			if (getSerializationFormat() != null)
				res.setSerializationFormat(getSerializationFormat());
			return res.serializeResults("CONSTRUCT");
		}
	}
	
	public String getDatasetMetadata(String datasetId) throws Exception {
		//check for existance of the Dataset
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(datasetId) == null)
			throw new Exception("Non-existing Dataset");
		else {
			QueryStatement query = StatementFactory.createQueryStatement();
			String queryString = "CONSTRUCT {?s ?p ?o} " +
									"WHERE {{" +
									"SELECT ?s ?p ?o FROM <" + datasetId +"> WHERE {?s ?p ?o}}}";
			Query q = new Query();
			q.setQueryText(queryString);
			q.setQueryType("CONSTRUCT");
			ArchiveResultSet res = query.executeQuery(q);		
			if (getSerializationFormat() != null)
				res.setSerializationFormat(getSerializationFormat());
			return res.serializeResults("CONSTRUCT");
		}
	}
	
	public String getChangeSetInfo(String changeSetId) {
		//TODO implement
		return null;
	}
	
	public String getRecord(String datasetId, List<String[]> parameters) throws Exception{
		QueryStatement query = StatementFactory.createQueryStatement();
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(datasetId) == null)
			throw new Exception("Non-existing Dataset");
		String queryString = "SELECT ?record WHERE {";
		queryString += "GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> { " +
						"<"+datasetId+"> <"+DiachronOntology.hasRecordSet+"> ?rs .}";
		queryString += "GRAPH ?rs { ?record a <"+DiachronOntology.record+"> . ";
		for(String[] param : parameters){
				String predicate = param[1];
				String object = param[2];
				try {
					predicate = "<"+new URL(predicate)+">";
			    } catch (Exception e1) {
			    	throw new Exception("Predicate is not a valid URI");
			    }
				try {
					object = "<"+new URL(object)+">";
			    } catch (Exception e1) {
			    	object = "\""+object+"\"";
			    }
				queryString += " ?record <"+DiachronOntology.hasRecordAttribute+"> [<"+DiachronOntology.predicate+"> "+predicate+" ; <"+DiachronOntology.object+"> "+object+" ] ."; 					
		}
		queryString += "}} ";
		
		
		Query q = new Query();
		q.setQueryText(queryString);
		q.setQueryType("SELECT");
		ArchiveResultSet res = query.executeQuery(q);
		if (getSerializationFormat() != null)
			res.setSerializationFormat(getSerializationFormat());
		
		String resultString = res.serializeResults(q.getQueryType());
		return resultString;
	}
	
	public String getResource(String datasetId, String resourceId) throws Exception {
		
		QueryStatement query = StatementFactory.createQueryStatement();
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDataset(datasetId) == null)
			throw new Exception("Non-existing Dataset");
		String queryString = "CONSTRUCT {<"+resourceId+"> ?p ?o} "
							+ "WHERE {";
		queryString += "GRAPH <"+RDFDictionary.getDictionaryNamedGraph()+"> { " +
						"<"+datasetId+"> <"+DiachronOntology.hasRecordSet+"> ?rs .}";
		queryString += "GRAPH ?rs { ?record <"+DiachronOntology.subject+"> <"+resourceId+"> ; "
				+ "<"+DiachronOntology.predicate+"> ?p ; "
				+ "<"+DiachronOntology.object+"> ?o .";
		queryString += "}}";
		/*for(String[] param : parameters){
				String predicate = param[1];
				String object = param[2];
				try {
					predicate = "<"+new URL(predicate)+">";
			    } catch (Exception e1) {
			    	throw new Exception("Predicate is not a valid URI");
			    }
				try {
					object = "<"+new URL(object)+">";
			    } catch (Exception e1) {
			    	object = "\""+object+"\"";
			    }
				queryString += " ?record <"+DiachronOntology.hasRecordAttribute+"> [<"+DiachronOntology.predicate+"> "+predicate+" ; <"+DiachronOntology.object+"> "+object+" ] ."; 					
		}
		queryString += "}} ";*/
		
		
		Query q = new Query();
		q.setQueryText(queryString);
		q.setQueryType("CONSTRUCT");
		ArchiveResultSet res = query.executeQuery(q);
		if (getSerializationFormat() != null)
			res.setSerializationFormat(getSerializationFormat());
		
		String resultString = res.serializeResults(q.getQueryType());
		return resultString;
	}

	/**
	 * @return the serializationFormat
	 */
	public SerializationFormat getSerializationFormat() {
		return serializationFormat;
	}

	/**
	 * @param serializationFormat the serializationFormat to set
	 */
	public void setSerializationFormat(SerializationFormat serializationFormat) {
		this.serializationFormat = serializationFormat;
	}
}		
/**
 * 
 * Instances of this class represent time conditions that can limit temporal queries.
 *
 */
abstract class Condition {
	public static enum OPERATOR {
		EQUAL, BEFORE, AFTER, BETWEEN 
	}
	/*
	private static enum JOINS {
		AND, OR
	}
	*/
	
	private OPERATOR operator = OPERATOR.EQUAL;
	/**
	 * @return the operator
	 */
	public OPERATOR getOperator() {
		return operator;
	}
	/**
	 * @param operator the operator to set
	 */
	public void setOperator(OPERATOR operator) {
		this.operator = operator;
	}
	
}

class VersionCondition extends Condition {
	private String value;
	private String value2;
	
	public VersionCondition(String versionURI) {
		setVersionURI(versionURI);
	}
	
	public VersionCondition(String versionURI, OPERATOR operator) throws Exception {
		if (operator == OPERATOR.BETWEEN)
			throw new Exception("Invalid constructor for this operator");
		setVersionURI(versionURI);
		setOperator(operator);
	}

	public VersionCondition(String from, String to) throws Exception {
		setVersionURI(from);
		setValue2(to);
		setOperator(OPERATOR.BETWEEN);
	}

	/**
	 * @return the versionURI
	 */
	public String getVersionURI() {
		return value;
	}

	/**
	 * @param versionURI the versionURI to set
	 */
	private void setVersionURI(String versionURI) {
		this.value = versionURI;
	}

	/**
	 * @return the value2
	 */
	public String getToVersionURI() {
		return value2;
	}

	/**
	 * @param value2 the value2 to set
	 */
	private void setValue2(String value2) {
		this.value2 = value2;
	}
}

class TimeCondition extends Condition {
	private Date value;
	private Date value2;
	
	public TimeCondition(Date date) {
		setDate(date);
	}
	
	public TimeCondition(Date date, OPERATOR operator) throws Exception {
		if (operator == OPERATOR.BETWEEN)
			throw new Exception("Invalid constructor for this operator");
		setDate(date);
		setOperator(operator);
	}
	
	public TimeCondition(Date from, Date to) throws Exception {
		setDate(from);
		value2 = to;
		setOperator(OPERATOR.BETWEEN);
	}


	/**
	 * @return the date
	 */
	public Date getDate() {
		return value;
	}

	public Date getToDate() {
		return value2;
	}

	
	/**
	 * @param date the date to set
	 */
	private void setDate(Date date) {
		this.value = date;
	}
}

class ParamCondition extends Condition {
	private String name;
	private Object value;
	
	public ParamCondition(String paramName, Object paramValue) {
		setParamValue(paramValue);
		setParamName(paramName);
	}
	
	/**
	 * @return the parameter value
	 */
	public Object getParamValue() {
		return value;
	}

	/**
	 * @param paramValue the parameter value to set
	 */
	private void setParamValue(Object paramValue) {
		this.value = paramValue;
	}
	
	/**
	 * @return the parameter name
	 */
	public String getParamName() {
		return name;
	}

	/**
	 * @param paramName the parameter name to set
	 */
	private void setParamName(String paramName) {
		this.name = paramName;
	}
}