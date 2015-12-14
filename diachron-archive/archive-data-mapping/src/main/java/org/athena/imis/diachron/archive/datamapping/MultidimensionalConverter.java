package org.athena.imis.diachron.archive.datamapping;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
        Model model = convert(in, ext, datasetName);
        try {
            model.write(out, "N3");
        } catch (Exception e) {
            logger.error("Could not write metadata into jena model.");
        } finally {
            model.close();
            try {
                out.close();
            } catch (IOException ignored) {
            }
        }
	}


    public Model convert(InputStream in, String ext, String datasetName) {
        StoreConnection.init();
        String version = (new Date()).getTime() + "";
        uriFactory = new DiachronURIFactory(datasetName, version);
        //Long lDateTime = new Date().getTime();
        //String tempName = lDateTime.toString().trim();
        try {
            if (BulkLoader.bulkLoadRDFDataToGraph(in, version, ext)) {
                //System.out.println("Converting temp graph " + tempName);
                logger.debug("Converting temp graph " + version);
                Model model = convertModel(version, datasetName);
                logger.debug("Uploading and housekeeping done.");
                return model;
            } else
                logger.error("Something went wrong.");

        } catch (Exception e) {
            logger.error("Could not convert from graph.", e);
        } finally {
            logger.debug("Clearing temp graph.");
            BulkLoader.clearStageGraph(version);
        }
        return null;
    }

    private Model convertModel(String fullGraph, String datasetName) {
        Model model = getDefaultModel();
        String schemaSetURI = diachronizeDataCubeSchema(model, fullGraph);
        String recordSetURI = diachronizeDataCubeObservations(model, fullGraph, datasetName);
        handleDatasetMetadata(model, schemaSetURI, recordSetURI);
        return model;
    }

    /**
     *  @param diachronModel
     * @param schemaSetURI URI of the created schema set
     * @param recordSetURI URI of the created record set
     *
     */
	public void handleDatasetMetadata(Model diachronModel, String schemaSetURI, String recordSetURI){
		
		VirtGraph graph = StoreConnection.getVirtGraph();

		String diachronicDatasetURI = uriFactory.generateDiachronicDatasetUri().toString();
		Resource diachronicDataset = diachronModel.createResource(diachronicDatasetURI, DiachronOntology.diachronicDataset);
		Resource datasetInstance = diachronModel.createResource(uriFactory.generateDatasetUri().toString(), DiachronOntology.dataset);		
		datasetInstance.addProperty(DiachronOntology.hasRecordSet, diachronModel.createResource(recordSetURI));
		datasetInstance.addProperty(DiachronOntology.hasSchemaSet, diachronModel.createResource(schemaSetURI));
		diachronicDataset.addProperty(DiachronOntology.hasInstantiation, datasetInstance);

		graph.close();
	}



    public static Map<String, String> prefixes = new HashMap<>();
    public static final String QB = "http://purl.org/linked-data/cube#";
    public static final String SKOS = "http://www.w3.org/2004/02/skos/core#";
    public static final String DIACHRON = "http://www.diachron-fp7.eu/resource/";
    public static final String DIACHRON_RECORD_ATTRIBUTE = "http://www.diachron-fp7.eu/resource/RecordAttribute/";
    public static final String DIACHRON_RECORD = "http://www.diachron-fp7.eu/resource/Record/";
    public static final String WSG84 = "http://www.w3.org/2003/01/geo/wgs84_pos#";

    public static final String BASE_PUBLICATION_URI = "http://www.data-publica.com/lod/publication/";

    static {
        prefixes.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        prefixes.put("owl", "http://www.w3.org/2002/07/owl#");
        prefixes.put("dp", BASE_PUBLICATION_URI);
        prefixes.put("wsg84", WSG84);
        prefixes.put("diachron", DIACHRON);
        prefixes.put("drca", DIACHRON_RECORD_ATTRIBUTE);
        prefixes.put("drc", DIACHRON_RECORD);
        prefixes.put("skos", SKOS);
        prefixes.put("qb", QB);
    }

    private Model getDefaultModel() {
        Model model = ModelFactory.createDefaultModel();
        model.setNsPrefixes(prefixes);
        return model;
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
							DiachronOntology.multidimensionalObservation)
					.addProperty(DiachronOntology.subject, obs.asResource());
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
	public String diachronizeDataCubeObservations(Model diachronModel,
                                                  String fullGraph, String datasetName) {

		VirtGraph graph = StoreConnection.getVirtGraph();
		
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

		diachronizeObservations(graph, fullGraph, diachronModel, recordSet);
		//diachronizeRestData(graph, fullGraph, diachronModel, recordSet);

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
	public String diachronizeDataCubeSchema(Model diachronModel,
                                            String fullGraph) {

		VirtGraph graph = StoreConnection.getVirtGraph();

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
		graph.close();
		return schemaSetURI;
	}

}
