package org.athena.imis.diachron.archive.datamapping;

import java.io.OutputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.athena.imis.diachron.archive.models.DiachronOntology;

import virtuoso.jdbc4.VirtuosoDataSource;
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

public class MultidimensionalConverter {
  
  private final VirtuosoDataSource dataSource;
  
  public MultidimensionalConverter(VirtuosoDataSource dataSource) {
    this.dataSource = dataSource;
  }

	public void convert(String fullGraph, OutputStream out) {		
		diachronizeDataCubeSchema(out, fullGraph);
		diachronizeDataCubeObservations(out, fullGraph);
		
	}
	
	
	public void diachronizeDataCubeObservations(OutputStream out, String fullGraph){
							
		VirtGraph graph = new VirtGraph(this.dataSource);
		
		String datasetQuery = "SELECT ?dataset FROM <"+fullGraph+"> WHERE {" +
				"?dataset a qb:DataSet" +
				"}";
		VirtuosoQueryExecution vqeD = VirtuosoQueryExecutionFactory.create (datasetQuery, graph);
		ResultSet resultsD = vqeD.execSelect();		
		// FIXME: what is the point of this while block???????
		String datasetID = "";
		while(resultsD.hasNext()){
			QuerySolution rs = resultsD.next();	
			RDFNode dataset = rs.get("dataset");
			datasetID = dataset.toString().substring(dataset.toString().lastIndexOf("/")+1);
		}
		vqeD.close();
		
		String obsQuery = "SELECT ?obs ?p ?o FROM <"+fullGraph+"> WHERE {" +
								"?obs a qb:Observation ; " +
								"?p ?o " +
							"}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (obsQuery, graph);
		ResultSet results = vqe.execSelect();
		Model diachronModel = ModelFactory.createDefaultModel();		
		while(results.hasNext()){
			QuerySolution rs = results.next();	
			RDFNode obs = rs.get("obs");
			String obsID = obs.toString().substring(obs.toString().lastIndexOf("/")+1);			
			Resource record = diachronModel.createResource(DiachronOntology.diachronResourcePrefix+"Record/"+obsID, DiachronOntology.record).addProperty(RDF.type, DiachronOntology.multidimensionalObservation);
			RDFNode p = rs.get("p");
			RDFNode o = rs.get("o");
			String rattID = DigestUtils.md5Hex(p.toString() + o.toString());
			Resource ratt = diachronModel.createResource(DiachronOntology.diachronResourcePrefix+"RecordAttribute/"+obsID+"/"+rattID, DiachronOntology.recordAttribute)
							.addProperty(DiachronOntology.predicate, p.asResource());
			if(o.isResource()) ratt.addProperty(DiachronOntology.object, o.asResource());
			else if (o.isLiteral()) ratt.addProperty(DiachronOntology.object, o.asLiteral());
			record.addProperty(DiachronOntology.hasRecordAttribute, ratt);
					
		}
		vqe.close();
		graph.close();
		try{ 			
			diachronModel.write(out, "RDF/XML-ABBREV");			
		} finally{
		  diachronModel.close();		  
		}
	}
	
	public void diachronizeDataCubeSchema(OutputStream out, String fullGraph){

        VirtGraph graph = new VirtGraph(this.dataSource);
		
		String datasetQuery = "SELECT ?dataset FROM <"+fullGraph+"> WHERE {" +
								"?dataset a qb:DataSet" +
								"}";
		VirtuosoQueryExecution vqeD = VirtuosoQueryExecutionFactory.create (datasetQuery, graph);
		ResultSet resultsD = vqeD.execSelect();		
		String datasetID = "";
		while(resultsD.hasNext()){
			QuerySolution rs = resultsD.next();	
			RDFNode dataset = rs.get("dataset");
			datasetID = dataset.toString().substring(dataset.toString().lastIndexOf("/")+1);
		}vqeD.close();
		String dsdQuery = "SELECT ?dsd ?p ?o ?codelist ?range FROM <"+fullGraph+"> WHERE {" +
										"?dsd a qb:DataStructureDefinition ; " +
										"qb:component [?p ?o] ." +										
										"OPTIONAL {?o qb:codeList ?codelist . }" +
										"OPTIONAL {?o rdfs:range ?range}" +
									"}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (dsdQuery, graph);
		ResultSet results = vqe.execSelect();
		Model diachronModel = ModelFactory.createDefaultModel();		
		while(results.hasNext()){
			QuerySolution rs = results.next();	
			Resource factTable = diachronModel.createResource(DiachronOntology.diachronResourcePrefix+"FactTable/"+datasetID, DiachronOntology.factTable);
			RDFNode p = rs.get("p");
			RDFNode o = rs.get("o");
			Resource dimProperty = null;
			if(p.toString().contains("dimension")){
			//if(rs.get("componentType").toString().contains("DimensionProperty")){
				 dimProperty= diachronModel.createResource(o.toString()).addProperty(RDF.type, DiachronOntology.dimensionProperty);
				factTable.addProperty(DiachronOntology.hasDimension, dimProperty);
			}
			else if(p.toString().contains("measure")){
			//if(rs.get("componentType").toString().contains("MeasureProperty")){
				dimProperty = diachronModel.createResource(o.toString()).addProperty(RDF.type, DiachronOntology.measureProperty);
				factTable.addProperty(DiachronOntology.hasMeasure, dimProperty);
			}
			if(rs.contains("codelist")){
				Resource codelist = diachronModel.createResource(rs.get("codelist").toString())
												 .addProperty(RDF.type, DiachronOntology.codeList);
				dimProperty.addProperty(DiachronOntology.codelist, codelist);
				String codelistQuery = "SELECT DISTINCT ?term ?p ?o FROM <"+fullGraph+"> " +
										"WHERE {" +
												"?term skos:inScheme <"+codelist.toString()+"> ; ?p ?o"+
											  "}";
				VirtuosoQueryExecution vqeCLQ = VirtuosoQueryExecutionFactory.create (codelistQuery, graph);
				ResultSet resultsCLQ = vqeCLQ.execSelect();					
				while(resultsCLQ.hasNext()){
					QuerySolution rsCLQ = resultsCLQ.next();
					RDFNode termCLQ = rsCLQ.get("term");
					Resource term = diachronModel.createResource(termCLQ.toString(), DiachronOntology.codeListTerm).addProperty(DiachronOntology.inScheme, codelist);
					RDFNode oCLQ = rsCLQ.get("o");
					if(oCLQ.isResource()) {
						term.addProperty(ResourceFactory.createProperty(rsCLQ.get("p").toString()), diachronModel.createResource(oCLQ.toString()));
						//System.out.println(oCLQ.toString());
					}
					else if (oCLQ.isLiteral()) {
						term.addProperty(ResourceFactory.createProperty(rsCLQ.get("p").toString()), oCLQ.toString()); 
					}
						
				}
			}
			if(rs.contains("range")){
				RDFNode range = rs.get("range");
				if(range.isResource())
					dimProperty.addProperty(RDFS.range, range.asResource());
				else if(range.isLiteral())
					dimProperty.addProperty(RDFS.range, range.toString());
			}
			//TODO check for attributes
			
		}
		vqe.close();		
		try{ 			
			diachronModel.write(out, "RDF/XML-ABBREV");			
		} finally {
		  diachronModel.close();		  
		}
	}

}
