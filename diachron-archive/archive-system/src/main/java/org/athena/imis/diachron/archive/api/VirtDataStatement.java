package org.athena.imis.diachron.archive.api;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Set;

import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;
import org.athena.imis.diachron.archive.core.dataloader.StoreFactory;
import org.athena.imis.diachron.archive.core.dataloader.DictionaryService;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.RDFDiachronicDataset;

/**
 * 
 * Implements the DataStatement interface for Virtuoso specific data statements.
 *
 */
public class VirtDataStatement implements DataStatement {

	/**
	 * Creates a new diachronic dataset and associates it with metadata defined in the input parameter.
	 * @param metadata A set of metadata to be associated with the new diachronic dataset.
	 */
	public String createDiachronicDataset(ArchiveEntityMetadata metadata){
		
		//TODO create with factory
		DiachronicDataset diachronicDataset = new RDFDiachronicDataset();
		HashMap<String, String> metadataMap = metadata.getMetadataMap();
		Set<String> keySet = metadataMap.keySet();
		
		for(String predicate : keySet){
			diachronicDataset.setMetaProperty(predicate, metadataMap.get(predicate));
		}
		DictionaryService dictService = StoreFactory.createDictionaryService();
		String URI = dictService.createDiachronicDataset(diachronicDataset);
		return URI;
		
	}

	/**
	 * Updates a diachronic dataset by loading new data to a Virtuoso instance associated with the archive store. 
	 * Accepts an InputStream object that contains the data to be loaded, and 
	 * a URI of the diachronic dataset to be updated.
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 */
	public void loadData(InputStream stream, String diachronicDatasetURI) {
		//stream = new FileInputStream("C:/Users/Marios/Desktop/datasetGraph.rdf");
		//TODO this should be read from the dictonary ???
		
		//This uploads directly the rdf file defined in the FileInputStream into namedGraph
		StoreFactory.createDataLoader().loadData(stream, diachronicDatasetURI);
		
	}
}
