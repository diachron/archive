package org.athena.imis.diachron.archive.core.dataloader;

import org.athena.imis.diachron.archive.core.datamanager.StoreConnection;
import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronicDataset;

import virtuoso.jena.driver.VirtGraph;
import virtuoso.jena.driver.VirtuosoUpdateFactory;
import virtuoso.jena.driver.VirtuosoUpdateRequest;

public class VirtRemover implements Remover {

	static DictionaryService dict = StoreFactory.createDictionaryService();
	
	@Override
	public void removeDataset(Dataset dataset) {
		// TODO Auto-generated method stub
		
		VirtGraph graph = StoreConnection.getVirtGraph();
		
		graph.setReadFromAllGraphs(true);
		
		//Delete the record set data
		String clearRecordSetQuery = " CLEAR GRAPH <"+dataset.getRecordSet().getId()+"> ";
		
		VirtuosoUpdateRequest vur = VirtuosoUpdateFactory.create(clearRecordSetQuery, graph);
		
		vur.exec();
		
		
		//Delete change sets (old and new)	
		if(dataset.getChangeSetOld() != null){
			
			String deleteChangeSetsQuery = " CLEAR GRAPH <"+dataset.getChangeSetOld()+"> ";
			
			vur = VirtuosoUpdateFactory.create(deleteChangeSetsQuery, graph);
			
			vur.exec();
			
		}
		
		if(dataset.getChangeSetNew() != null){
			
			String deleteChangeSetsQuery = " CLEAR GRAPH <"+dataset.getChangeSetNew()+"> ";
			
			vur = VirtuosoUpdateFactory.create(deleteChangeSetsQuery, graph);
			
			vur.exec();
			
		}
		
		
		String updateDictionaryQuery = " DELETE FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> "
				+ " { <"+dataset.getId()+"> ?p ?o . } "
				+ " WHERE  { <"+dataset.getId()+"> ?p ?o . } ";
				
		vur = VirtuosoUpdateFactory.create(updateDictionaryQuery, graph);
		
		vur.exec();
		
		updateDictionaryQuery = " DELETE FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> "
				+ " { <"+dataset.getRecordSet().getId()+"> ?p ?o . } "
				+ " WHERE  { <"+dataset.getRecordSet().getId()+"> ?p ?o . } ";
				
		vur = VirtuosoUpdateFactory.create(updateDictionaryQuery, graph);
		
		vur.exec();
		
		updateDictionaryQuery = " DELETE FROM <"+RDFDictionary.getDictionaryNamedGraph()+"> "
				+ " { ?d ?p <"+dataset.getId()+">  } "
				+ " WHERE { ?d ?p <"+dataset.getId()+">  } ";
				
		vur = VirtuosoUpdateFactory.create(updateDictionaryQuery, graph);
		
		vur.exec();
				
		//String clearSchemaSetQuery = " CLEAR GRAPH <"+dataset.getSchemaSet().getId()+"> ";
		
		graph.close();

	}

	@Override
	public void removeDiachronicDataset(DiachronicDataset diachronicDataset) {
		
		for(Dataset dataset : dict.getListOfDatasets(diachronicDataset)){
			
			removeDataset(dataset);
			
		}

	}

}
