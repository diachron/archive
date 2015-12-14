package org.athena.imis.diachron.archive.api;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Instances of the class ArchiveResultSet represent results of a query defined in a Query object. 
 * Archive result sets can either be Jena result sets (e.g. from a SELECT query), JDBC result sets   
 * or Jena models (e.g. from a CONSTRUCT query).  
 *
 */
public class ArchiveResultSet {
	public enum SerializationFormat {
		RDFXML, RDFJSON;
	
		protected String getString() {
			switch (this) {
			case RDFXML: return "RDF/XML";
			case RDFJSON: return "RDF/JSON";
			default: return "";
			}
		}
	};
	//private static SerializationFormat defaultSerialize = SerializationFormat.RDFXML;
	private static SerializationFormat defaultSerialize = SerializationFormat.RDFJSON;
	private SerializationFormat serializationFormat = defaultSerialize;
	
	private ResultSet jdbcResult;
	private com.hp.hpl.jena.query.ResultSet jenaResultSet;
	private Model jenaModel;

	public SerializationFormat getSerializationFormat() {
		return serializationFormat;
	}
	public SerializationFormat setSerializationFormat(SerializationFormat serializationFormat) {
		return this.serializationFormat = serializationFormat;
	}
	
	/**
	 * Fetches the Jena Model associated with this ArchiveResultSet object.
	 * @return the Jena Model of the RDF result from a CONSTRUCT query, null if one does not exist
	 */
	public Model getJenaModel() {
		return jenaModel;
	}

	/**
	 * Fetches the JDBC result set associated with this ArchiveResultSet object.
	 * @return the jdbc result set of a generic query, null if not set.
	 */
	public ResultSet getJdbcResult() {
		return jdbcResult;
	}

	/**
	 * Sets the jdbc result set of this ArchiveResultSet object. 
	 * @param jdbcResult the jdbc result set to be associated with this ArchiveResultSet object.
	 */
	public void setJdbcResult(ResultSet jdbcResult) {
		this.jdbcResult = jdbcResult;
	} 
		
	/**
	 * Gets the Jena result set (if any) associated with this ArchiveResultSet object. 
	 * @return A jena result set of a SELECT query, null if not set.
	 */
	public com.hp.hpl.jena.query.ResultSet getJenaResultSet(){
		return this.jenaResultSet;
	}
	
	/**	
	 * Sets the jena result set of this ArchiveResultSet object. 	 
	 * @param jenaResultSet The jena result set to be associated with this ArchiveResultSet object.
	 */
	public void setJenaResultSet(com.hp.hpl.jena.query.ResultSet jenaResultSet){
		
		this.jenaResultSet = ResultSetFactory.copyResults(jenaResultSet);		
	}
	
	/**
	 * Serializes the results of the Jena result set (if any) of this ArchiveResultSet object in a string.
	 * @return A string containing a serialization of the ArchiveResultSet's jena result set as RDF/JSON.
	 */
	public String serializeJenaResultSet(){
		
		if (getSerializationFormat()==SerializationFormat.RDFJSON) {
			/*Model model = ModelFactory.createDefaultModel();
			ResultSetFormatter.asRDF(model, jenaResultSet);		
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			model.write(os, "RDF/JSON");*/
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(os, jenaResultSet);
			return os.toString();
		} else if (getSerializationFormat()==SerializationFormat.RDFXML) {
			return ResultSetFormatter.asXMLString(jenaResultSet);		
		} else {
			return null;
		}
	}
	
	/**
	 * Sets the jena model of this ArchiveResultSet object. 
	 * @param jenaModel A jena model containing RDF from a CONSTRUCT query.
	 */
	public void setJenaModel(Model jenaModel){
		this.jenaModel = jenaModel;
	}
	
	/**
	 * Serializes the results of the Jena Model (if any) of this ArchiveResultSet object in a string.
	 * @return A string containing a serialization of the ArchiveResultSet's jena model as RDF/JSON.
	 */
	public String serializeJenaModel(){
		StringWriter out = new StringWriter();
		jenaModel.write(out, getSerializationFormat().getString());
		return out.toString();
		
	}
	
	/**
	 * Serializes the result set that has been set for this ArchiveResultSet object. This method accepts a 
	 * parameter to decide the query type and depending on the query type it serializes a Jena Model or 
	 * a Jena result set.
	 * @param queryType The type of the executed query whose results are to be serialized. 
	 * @return A string containing a serialization of the ArchiveResultSet's results as RDF/JSON.
	 */
	public String serializeResults(String queryType){
		if (queryType.equals("SELECT")) return serializeJenaResultSet();
		else if (queryType.equals("CONSTRUCT")) return serializeJenaModel();
		else return null;
	}
}
