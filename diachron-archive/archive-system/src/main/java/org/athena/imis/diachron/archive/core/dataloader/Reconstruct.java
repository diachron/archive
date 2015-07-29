package org.athena.imis.diachron.archive.core.dataloader;

import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.codec.digest.DigestUtils;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronURIFactory;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.SimpleChange;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class Reconstruct {

	
	private Model added;
	private Model deleted;
	//private Model full;
	String changeSetURI;
	String fmModelURI;
	DiachronicDataset requestedDataset;
	
	public Reconstruct(String changeSetURI, DiachronicDataset diachronic) throws ReconstructException { 
						
		this.changeSetURI = changeSetURI;
		
		requestedDataset = diachronic;
		
		if(changeSetURI == null || changeSetURI.equals("")) throw new ReconstructException();	
		
		added = ModelFactory.createDefaultModel();
		
		deleted = ModelFactory.createDefaultModel();
		
		populateModels();
		
	}
		
	
	private void populateModels(){
		
		String query = " SELECT ?s ?o1 ?o2 ?o3 ?type "
						+ " FROM <" + changeSetURI + "> "
						+ " WHERE { ?s a ?type . "
						+ "?s ?p1 ?o1 . FILTER REGEX(str(?p1), \"_p1\")"
						+ "OPTIONAL {?s ?p2 ?o2 FILTER REGEX(str(?p2), \"_p2\")}"
						+ "OPTIONAL {?s ?p3 ?o3 FILTER REGEX(str(?p3), \"_p3\")}" 
						+ "}";		
		Model model = StoreConnection.getJenaModel(changeSetURI);
		QueryExecution vqe = QueryExecutionFactory.create(query, model);
		ResultSet results = vqe.execSelect();
		while(results.hasNext()){
			QuerySolution rs = results.next();
			try {
				Node o1 = null, o2 = null, o3 = null;
				o1 = rs.get("o1").asNode();
				if(rs.contains("o2"))
					o2 = rs.get("o2").asNode();
				if(rs.contains("o3"))
					o3 = rs.get("o3").asNode();
				SimpleChange simpleChange = new SimpleChange(rs.get("type").toString(), o1, o2, o3);				
				if(simpleChange.getAddOrDel().equals("add")){
					
					//added.add(added.asStatement(simpleChange.getTriple()));
					generateRecord(simpleChange.getTriple(), added);
				}
					
				else{
					
					//deleted.add(added.asStatement(simpleChange.getTriple()));
					generateRecord(simpleChange.getTriple(), deleted);
					
				}
					
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
		
		model.close();		
		
	}
	
	public Model getAddedModel(){
		return added;
	}
	
	public Model getDeletedModel(){
		return deleted;
	}	
		
	
	public void writeAddedModel(OutputStream out){
				
			added.write(out);
			
	}
	
	public void writeDeletedModel(OutputStream out){
		
		deleted.write(out);
		
	}
	
	
	public void generateRecord(Triple triple, Model diachronModel) throws Exception{
						
		DiachronURIFactory uriFactory = new DiachronURIFactory("efo", "");
		
		String recordID = uriFactory.generateRecordUriString(triple.getSubject().getURI().toString()).toString();	
		Resource record = diachronModel.createResource(recordID, DiachronOntology.record);
		record.addProperty(DiachronOntology.subject, diachronModel.createResource(triple.getSubject().getURI()));		
		String rattID = DigestUtils.md5Hex(triple.getPredicate().toString() + triple.getObject().toString());
		if(triple.getObject().isURI()) 
			rattID = uriFactory.generateRecordAttributeUriString(
					triple.getSubject().getURI(), triple.getPredicate().getURI().toString(), triple.getObject().getURI().toString()).toString();
		else 
			rattID = uriFactory.generateRecordAttributeUriString(triple.getSubject().getURI(), triple.getPredicate().getURI().toString(), triple.getObject().getLiteral().toString()).toString();
		Resource ratt = diachronModel.createResource(rattID, DiachronOntology.recordAttribute)
					.addProperty(DiachronOntology.predicate, ResourceFactory.createResource(triple.getPredicate().getURI()));
		if(triple.getObject().isURI()) 
			ratt.addProperty(DiachronOntology.object, diachronModel.createResource(triple.getObject().getURI()));
		else 
			ratt.addProperty(DiachronOntology.object, triple.getObject().getLiteral().toString());					
		record.addProperty(DiachronOntology.hasRecordAttribute, ratt);
		
		
	}
}
