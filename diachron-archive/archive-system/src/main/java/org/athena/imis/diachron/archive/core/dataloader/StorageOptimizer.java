package org.athena.imis.diachron.archive.core.dataloader;

import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.models.DiachronOntology;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class StorageOptimizer {
	
	/*public StorageOptimizer(String diachronicDatasetId) {
		
	}*/
	
	private int numberOfRecords, numberOfSubjects;
	private int numberOfRecordAttributes;
	private int numberOfStatements;
	private int numberOfPredicates;
	private int numberOfObjects;
	private String tempGraph;
	private double sizeScore;
	private int strategy;
	
	public StorageOptimizer(String tempGraph) {
		
		this.tempGraph = tempGraph;
		analyze();
	}
	
	
	public void analyze() {
		
		numberOfStatements = getSize();
		
		numberOfRecords = getRecordStatistics();
		
		numberOfSubjects = numberOfRecords;
		
		numberOfRecordAttributes = getRecordAttributeStatistics();
		
		numberOfPredicates = setNumberOfPredicates();
		
		numberOfObjects = setNumberOfObjects();
		
		sizeScore = computeSizeScore();
		
	}
	
	public String suggestStrategy() {
		return "";
	}
	
	public void applyStrategy() {
		
		if(sizeScore<10) strategy = 1;
		else strategy = 0;
		
	}
	
	public int getStrategy(){
		
		return strategy;
		
	}
	
	public double computeSizeScore(){
		
		double score = -1.0;
		double recordDensity = numberOfRecordAttributes/numberOfRecords;
		double subjectDensity = numberOfSubjects/numberOfRecordAttributes;
		double objectDensity = numberOfObjects/numberOfSubjects;
		double predicateDensity = numberOfPredicates/numberOfRecords;
		//score = recordDensity + objectDensity + predicateDensity + subjectDensity;
		score = recordDensity*0.5 + objectDensity*0.3 + predicateDensity*0.2;
		
		return score;
		
	}
	
	
	/**
	 * Returns the number of reified triples in the temp graph. 
	 * 
	 * @return An int representation of the number of reified triples.
	 */
	public int getSize(){
		
		Model model = StoreConnection.getJenaModel(tempGraph);
		String query = "SELECT (count(*) as ?count) FROM <" + tempGraph + "> WHERE {?s ?p ?o}";
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		int size = -1;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			size  = rs.getLiteral("count").getInt();
		}
		
		return size;
	}
	
	/**
	 * Returns the number of record elements (DIACHRON records) in the temp graph. 
	 * 
	 * @return An int representation of the number of records.
	 */
	public int getRecordStatistics(){
				
		Model model = StoreConnection.getJenaModel(tempGraph);
		String query = "SELECT (count(DISTINCT ?rec) as ?count) FROM <" + tempGraph + "> WHERE {?rec <"+DiachronOntology.hasRecordAttribute+"> ?ratt}";
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		int size = -1;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			size  = rs.getLiteral("count").getInt();
		}
		
		return size;
	}
	
	/**
	 * Returns the number of record attribute elements (DIACHRON record attributes) in the temp graph. 
	 * 
	 * @return An int representation of the number of record attributes.
	 */
	public int getRecordAttributeStatistics(){
		
		Model model = StoreConnection.getJenaModel(tempGraph);
		String query = "SELECT (count(DISTINCT ?ratt) as ?count) FROM <" + tempGraph + "> WHERE {?rec <"+DiachronOntology.hasRecordAttribute+"> ?ratt}";
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		int size = -1;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			size  = rs.getLiteral("count").getInt();
		}
		
		return size;
	}
	
	/**
	 * Returns the number of distinct objects in the temp graph. 
	 * 
	 * @return An int representation of the number of distinct objects in the de-reified triples.
	 */
	public int setNumberOfObjects(){
		
		Model model = StoreConnection.getJenaModel(tempGraph);
		String query = "SELECT (count(DISTINCT ?o) as ?count) FROM <" + tempGraph + "> WHERE {?rec <"+DiachronOntology.object+"> ?o}";
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		int size = -1;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			size  = rs.getLiteral("count").getInt();
		}
		
		return size;
	}
	
	/**
	 * Returns the number of distinct predicates in the temp graph. 
	 * 
	 * @return An int representation of the number of distinct predicates in the de-reified triples.
	 */
	public int setNumberOfPredicates(){
		
		Model model = StoreConnection.getJenaModel(tempGraph);
		String query = "SELECT (count(DISTINCT ?p) as ?count) FROM <" + tempGraph + "> WHERE {?rec <"+DiachronOntology.predicate+"> ?p}";
		QueryExecution vqe = QueryExecutionFactory.create (query, model);
		ResultSet results = vqe.execSelect();		
		int size = -1;
		while(results.hasNext()){
			QuerySolution rs = results.next();
			size  = rs.getLiteral("count").getInt();
		}
		
		return size;
	}
	
	
	
	public int getNumberOfSubjects(){
		return numberOfRecords;
	}
	
	public int getNumberOfPredicates(){
		return numberOfPredicates;
	}
	
	public int getNumberOfObjects(){
		return numberOfObjects;
	}
	
	public int getNumberOfRecordAttributes(){
		return numberOfRecordAttributes;
	}
	

}
