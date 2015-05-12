package org.athena.imis.diachron.archive.datamapping;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

import openlink.util.MD5;

import org.apache.commons.codec.digest.DigestUtils;
import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.datamapping.utils.BulkLoader;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronURIFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.jena.driver.VirtuosoQueryExecution;
import virtuoso.jena.driver.VirtuosoQueryExecutionFactory;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

public class RDFConverter implements DataConverter {

	private static final Logger logger = LoggerFactory.getLogger(MultidimensionalConverter.class);
	//private DiachronicDataset diachronicDataset;
	private DiachronURIFactory uriFactory;
	
	public void convert(InputStream input, OutputStream out) {
		// TODO Auto-generated method stub

	}

	
	public void convert(InputStream input, OutputStream out, String datasetName, String ext) {
		// TODO Auto-generated method stub
		StoreConnection.init();
		String version = (new Date()).getTime() + "";
		uriFactory = new DiachronURIFactory(datasetName, version);
		//Long lDateTime = new Date().getTime();
		//String tempName = lDateTime.toString().trim();		
		if (BulkLoader.bulkLoadRDFDataToGraph(input, version, ext)) {
			//System.out.println("Converting temp graph " + version);
			try{
				logger.debug("Converting temp graph " + version);
				convertFromGraph(version, out);
			} catch(Exception e){
				logger.error("Could not convert from graph.");
				e.printStackTrace();
			}
			
			logger.debug("Clearing temp graph.");
			BulkLoader.clearStageGraph(version);
			
			logger.debug("Uploading and housekeeping done.");
		} else
			logger.error("Something went wrong.");
		
	}
	
	public static void convertFromGraph(String fullGraph, OutputStream out) throws Exception{
		
		DiachronURIFactory uriFactory = new DiachronURIFactory(fullGraph, "");
		String obsQuery = "SELECT ?s ?p ?o FROM <"+fullGraph+"> WHERE {" +
				"?s ?p ?o " +
			"}";
		VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (obsQuery, StoreConnection.getVirtGraph());
		ResultSet results = vqe.execSelect();
		Model diachronModel = ModelFactory.createDefaultModel();		
		while(results.hasNext()){
		QuerySolution rs = results.next();	
		RDFNode obs = rs.get("s");
		
		String obsID = obs.toString().substring(obs.toString().lastIndexOf("/")+1);
		obsID = uriFactory.generateRecordUri(new URI(obsID)).toString();	
		Resource record = diachronModel.createResource(obsID, DiachronOntology.record);
		record.addProperty(DiachronOntology.subject, obs.toString());
		RDFNode p = rs.get("p");
		RDFNode o = rs.get("o");
		String rattID = DigestUtils.md5Hex(p.toString() + o.toString());
		if(o.isResource()) 
			rattID = uriFactory.generateRecordAttributeUri(new URI(rattID), new URI(p.toString()), new URI(o.toString())).toString();
		else 
			rattID = uriFactory.generateRecordAttributeUri(new URI(rattID), new URI(p.toString()), o.toString()).toString();
		Resource ratt = diachronModel.createResource(rattID, DiachronOntology.recordAttribute)
					.addProperty(DiachronOntology.predicate, p.asResource());
		if(o.isResource()) 
		ratt.addProperty(DiachronOntology.object, o.asResource());
		else if (o.isLiteral()) 
		ratt.addProperty(DiachronOntology.object, o.asLiteral());
		record.addProperty(DiachronOntology.hasRecordAttribute, ratt);
			
		}vqe.close();
		try{ 			
			diachronModel.write(out, "RDF/XML-ABBREV");			
		}catch(Exception e){}
		diachronModel.close();		
	}


	@Override
	public void convert(InputStream input, OutputStream out, String datasetName) {
		// TODO Auto-generated method stub
		
	}
	

}
