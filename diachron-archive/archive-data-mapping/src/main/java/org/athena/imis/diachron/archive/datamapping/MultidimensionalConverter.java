package org.athena.imis.diachron.archive.datamapping;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.datamapping.utils.BulkLoader;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronURIFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * 
 * This class handles conversion of Multidimensional datasets, 
 * expressed in the RDF Data Cube vocabulary, to the DIACHRON model. 
 *
 */
public class MultidimensionalConverter {

	private static final Logger logger = LoggerFactory.getLogger(MultidimensionalConverter.class);
	//private DiachronicDataset diachronicDataset;
	private DiachronURIFactory uriFactory;
	
	
	/**
	 * 
	 * @param in InputStream object with the serialized RDF dataset expressed in Data Cube. 
	 * @param out OutputStream object that will contain the serialized DIACHRON model data after conversion.
	 * @param ext Extension of the serialization to be given to the bulk loader.
	 * @param datasetName Unique string identification of the dataset.
	 * 
	 * This method is given an input stream with the input dataset which converts and serializes into the given output stream. 
	 */
	public void convert(InputStream in, OutputStream out, String ext, String datasetName) {

		StoreConnection.init();
		String version = (new Date()).getTime() + "";
		uriFactory = new DiachronURIFactory(datasetName, version);
		//Long lDateTime = new Date().getTime();
		//String tempName = lDateTime.toString().trim();		
		if (BulkLoader.bulkLoadRDFDataToGraph(in, version, ext)) {
			//System.out.println("Converting temp graph " + tempName);
			try{
				logger.debug("Converting temp graph " + version);
				convertFromGraph(version, out, datasetName);
			} catch(IOException e){
				logger.error("Could not convert from graph.");
			}
			
			logger.debug("Clearing temp graph.");
			BulkLoader.clearStageGraph(version);
			
			logger.debug("Uploading and housekeeping done.");
		} else
			logger.error("Something went wrong.");

	}

	/**
	 * 
	 * @param fullGraph The temp graph used to bulk load the data into the Virtuoso store.
	 * @param out The output stream object passed along from the calling method.
	 * @param datasetName The unique identifier name of the dataset.
	 * @throws IOException
	 */
	public void convertFromGraph(String fullGraph, OutputStream out, String datasetName) throws IOException {
		
		String schemaSetURI = diachronizeDataCubeSchema(out, fullGraph);
		String recordSetURI = diachronizeDataCubeObservations(out, fullGraph, datasetName);
		handleDatasetMetadata(out, datasetName, schemaSetURI, recordSetURI);
		out.close();
		
	}

	/**
	 * 
	 * @param out The output stream object passed along from the calling method.
	 * @param datasetName The unique identifier name of the dataset.
	 * @param schemaSetURI URI of the created schema set
	 * @param recordSetURI URI of the created record set
	 * 
	 * This method creates metadata specific to the Dictionary of datasets within the archive.
	 */
	public void handleDatasetMetadata(OutputStream out, String datasetName, String schemaSetURI, String recordSetURI){
		
		VirtGraph graph = StoreConnection.getVirtGraph();
		
		DiachronURIFactory uriFactory = new DiachronURIFactory(datasetName, "");
		String diachronicDatasetURI = uriFactory.generateDiachronicDatasetUri().toString();
		Model diachronModel = ModelFactory.createDefaultModel();
		Resource diachronicDataset = diachronModel.createResource(diachronicDatasetURI, DiachronOntology.diachronicDataset);
		Resource datasetInstance = diachronModel.createResource(uriFactory.generateDatasetUri().toString(), DiachronOntology.dataset);		
		datasetInstance.addProperty(DiachronOntology.hasRecordSet, diachronModel.createResource(recordSetURI));
		datasetInstance.addProperty(DiachronOntology.hasSchemaSet, diachronModel.createResource(schemaSetURI));
		diachronicDataset.addProperty(DiachronOntology.hasInstantiation, datasetInstance);
		
		try {
			diachronModel.write(out, "RDF/XML-ABBREV");
		} catch (Exception e) {
			logger.error("Could not write metadata into jena model.");
		}
		diachronModel.close();
		graph.close();
	}
	
	/**
	 * 
	 * @param graph VirtGraph jdbc connection to the archive.
	 * @param fullGraph Temp graph name.
	 * @param diachronModel Jena model object with the dataset under conversion.
	 * @param recordSet The created jena resource for the record set.
	 * @return The modified diachronModel object.
	 * 
	 * This method converts multidimensional observations and appends them in the converted jena model.
	 */
	public Model diachronizeObservations(VirtGraph graph, String fullGraph, Model diachronModel, Resource recordSet){
		String obsQuery = "SELECT ?obs ?p ?o FROM <" + fullGraph + "> WHERE {"
				+ "?obs a qb:Observation ; " + "?p ?o " + "}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(
				obsQuery, graph);
		ResultSet results = vqe.execSelect();
		
		while (results.hasNext()) {
			QuerySolution rs = results.next();
			RDFNode obs = rs.get("obs");
			String obsID = obs.toString().substring(
					obs.toString().lastIndexOf("/") + 1);
			Resource record = diachronModel
					.createResource(
							DiachronOntology.diachronResourcePrefix + "Record/"
									+ obsID, DiachronOntology.record)
					.addProperty(RDF.type,
							DiachronOntology.multidimensionalObservation);
			RDFNode p = rs.get("p");
			RDFNode o = rs.get("o");
			String rattID = DigestUtils.md5Hex(p.toString() + o.toString());
			Resource ratt = diachronModel.createResource(
					DiachronOntology.diachronResourcePrefix
							+ "RecordAttribute/" + obsID + "/" + rattID,
					DiachronOntology.recordAttribute).addProperty(
					DiachronOntology.predicate, p.asResource());
			if (o.isResource())
				ratt.addProperty(DiachronOntology.object, o.asResource());
			else if (o.isLiteral())
				ratt.addProperty(DiachronOntology.object, o.asLiteral());
			record.addProperty(DiachronOntology.hasRecordAttribute, ratt);
			recordSet.addProperty(DiachronOntology.hasRecord, record);
		}
		vqe.close();
		return diachronModel;
	}
	
	/**
	 * 
	 * @param graph VirtGraph jdbc connection to the archive.
	 * @param fullGraph Temp graph name.
	 * @param diachronModel Jena model object with the dataset under conversion.
	 * @param recordSet The created jena resource for the record set.
	 * @return The modified diachronModel object.
	 * 
	 * This method converts data in the dataset other than schema and observations, and appends them in the converted jena model.
	 */
	public Model diachronizeRestData(VirtGraph graph, String fullGraph, Model diachronModel, Resource recordSet){
		
		String allQuery = "SELECT ?s ?p ?o FROM <" + fullGraph + "> WHERE {"
				+ "?s a ?type ; ?p ?o FILTER(?type!=qb:Observation)" + "}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(allQuery, graph);
		ResultSet results = vqe.execSelect();
		while (results.hasNext()) {
			QuerySolution rs = results.next();
			RDFNode obs = rs.get("s");
			String obsID = DigestUtils.md5Hex(obs.toString());
			Resource record = diachronModel
					.createResource(DiachronOntology.diachronResourcePrefix
							+ "Record/" + obsID, DiachronOntology.record);
			RDFNode p = rs.get("p");
			RDFNode o = rs.get("o");
			String rattID = DigestUtils.md5Hex(p.toString() + o.toString());
			Resource ratt = diachronModel.createResource(
					DiachronOntology.diachronResourcePrefix
							+ "RecordAttribute/" + obsID + "/" + rattID,
					DiachronOntology.recordAttribute).addProperty(
					DiachronOntology.predicate, p.asResource());
			if (o.isResource())
				ratt.addProperty(DiachronOntology.object, o.asResource());
			else if (o.isLiteral())
				ratt.addProperty(DiachronOntology.object, o.asLiteral());
			record.addProperty(DiachronOntology.hasRecordAttribute, ratt);
			recordSet.addProperty(DiachronOntology.hasRecord, record);
		}
		vqe.close();
		
		return diachronModel;
		
	}
	
	/**
	 * 
	 * Caller class for converting data objects (records).
	 */
	public String diachronizeDataCubeObservations(OutputStream out,
			String fullGraph, String datasetName) {

		VirtGraph graph = StoreConnection.getVirtGraph();
		Model diachronModel = ModelFactory.createDefaultModel();
		
		String recordSetURI = uriFactory.generateDiachronRecordSetURI().toString();
		Resource recordSet = diachronModel.createResource(recordSetURI, DiachronOntology.recordSet);
		
		/*String datasetQuery = "SELECT ?dataset FROM <" + fullGraph
				+ "> WHERE {" + "?dataset a qb:DataSet" + "}";
		VirtuosoQueryExecution vqeD = VirtuosoQueryExecutionFactory.create(
				datasetQuery, graph);
		ResultSet resultsD = vqeD.execSelect();
		// String datasetID = "";
		//TODO Is this loop needed any more? remove it??
		while (resultsD.hasNext()) {
			QuerySolution rs = resultsD.next();
			RDFNode dataset = rs.get("dataset");
			// datasetID =dataset.toString().substring(dataset.toString().lastIndexOf("/") + 1);
		}
		vqeD.close();*/

		diachronModel = diachronizeObservations(graph, fullGraph, diachronModel, recordSet);
		diachronModel = diachronizeRestData(graph, fullGraph, diachronModel, recordSet);
		
		try {
			diachronModel.write(out, "RDF/XML-ABBREV");
		} catch (Exception e) {
			logger.error("Could not write Observations model in output stream.");
		}
		diachronModel.close();
		graph.close();
		return recordSetURI;
	}
	
	/**
	 * 
	 * @param graph VirtGraph jdbc connection to the archive.
	 * @param fullGraph Temp graph name.
	 * @return The ID of the dataset found in the Data Cube input.
	 */
	public String getDatasetID(VirtGraph graph, String fullGraph){
		
		String datasetQuery = "SELECT ?dataset FROM <" + fullGraph
				+ "> WHERE {" + "?dataset a qb:DataSet" + "}";
		VirtuosoQueryExecution vqeD = VirtuosoQueryExecutionFactory.create(
				datasetQuery, graph);
		ResultSet resultsD = vqeD.execSelect();
		String datasetID = "";
		while (resultsD.hasNext()) {
			QuerySolution rs = resultsD.next();
			RDFNode dataset = rs.get("dataset");
			datasetID = dataset.toString().substring(
					dataset.toString().lastIndexOf("/") + 1);
		}
		vqeD.close();
		return datasetID;
	}

	/**
	 * 
	 * Caller class for converting schema objects.
	 */
	public String diachronizeDataCubeSchema(OutputStream out,
			String fullGraph) {

		VirtGraph graph = StoreConnection.getVirtGraph();
		
		Model diachronModel = ModelFactory.createDefaultModel();
		String schemaSetURI = uriFactory.generateDiachronSchemaSetURI().toString();
		Resource schemaSet = diachronModel.createResource(schemaSetURI, DiachronOntology.schemaSet);
		
		
		String datasetID = getDatasetID(graph, fullGraph);
		
		String dsdQuery = "SELECT ?dsd ?p ?o ?codelist ?range FROM <"
				+ fullGraph + "> WHERE {"
				+ "?dsd a qb:DataStructureDefinition ; "
				+ "qb:component [?p ?o] ."
				+ "OPTIONAL {?o qb:codeList ?codelist . }"
				+ "OPTIONAL {?o rdfs:range ?range}" + "}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(
				dsdQuery, graph);
		ResultSet results = vqe.execSelect();
		
		while (results.hasNext()) {
			QuerySolution rs = results.next();
			// RDFNode dsd = rs.get("dsd");
			Resource factTable = diachronModel.createResource(
					DiachronOntology.diachronResourcePrefix + "FactTable/"
							+ datasetID, DiachronOntology.factTable);
			schemaSet.addProperty(DiachronOntology.hasFactTable, factTable);
			RDFNode p = rs.get("p");
			RDFNode o = rs.get("o");
			Resource dimProperty = null;
			if (p.toString().contains("dimension")) {
				// if(rs.get("componentType").toString().contains("DimensionProperty")){
				dimProperty = diachronModel.createResource(o.toString())
						.addProperty(RDF.type,
								DiachronOntology.dimensionProperty);
				factTable.addProperty(DiachronOntology.hasDimension,
						dimProperty);
			} else if (p.toString().contains("measure")) {
				// if(rs.get("componentType").toString().contains("MeasureProperty")){
				dimProperty = diachronModel
						.createResource(o.toString())
						.addProperty(RDF.type, DiachronOntology.measureProperty);
				factTable.addProperty(DiachronOntology.hasMeasure, dimProperty);
			}
			if (rs.contains("codelist")) {
				Resource codelist = diachronModel.createResource(
						rs.get("codelist").toString()).addProperty(RDF.type,
						DiachronOntology.codeList);
				dimProperty.addProperty(DiachronOntology.codelist, codelist);
				String codelistQuery = "SELECT DISTINCT ?term ?p ?o FROM <"
						+ fullGraph + "> " + "WHERE {"
						+ "?term skos:inScheme <" + codelist.toString()
						+ "> ; ?p ?o" + "}";
				VirtuosoQueryExecution vqeCLQ = VirtuosoQueryExecutionFactory
						.create(codelistQuery, graph);
				ResultSet resultsCLQ = vqeCLQ.execSelect();
				while (resultsCLQ.hasNext()) {
					QuerySolution rsCLQ = resultsCLQ.next();
					RDFNode termCLQ = rsCLQ.get("term");
					Resource term = diachronModel.createResource(
							termCLQ.toString(), DiachronOntology.codeListTerm)
							.addProperty(DiachronOntology.inScheme, codelist);
					RDFNode oCLQ = rsCLQ.get("o");
					if (oCLQ.isResource()) {
						term.addProperty(ResourceFactory.createProperty(rsCLQ
								.get("p").toString()), diachronModel
								.createResource(oCLQ.toString()));
						// System.out.println(oCLQ.toString());
					} else if (oCLQ.isLiteral()) {
						term.addProperty(ResourceFactory.createProperty(rsCLQ
								.get("p").toString()), oCLQ.toString());
					}

				}
			}
			if (rs.contains("range")) {
				RDFNode range = rs.get("range");
				if (range.isResource())
					dimProperty.addProperty(RDFS.range, range.asResource());
				else if (range.isLiteral())
					dimProperty.addProperty(RDFS.range, range.toString());
			}
			// TODO check for attributes

		}
		vqe.close();
		try {
			diachronModel.write(out, "RDF/XML-ABBREV");
		} catch (Exception e) {
			logger.error("Could not write Observations model in output stream.");
		}
		diachronModel.close();
		graph.close();
		return schemaSetURI;
	}

}
