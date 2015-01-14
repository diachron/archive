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
public class BasicDataStatement implements DataStatement {

	/**
	 * Creates a new diachronic dataset and associates it with metadata defined in the input parameter.
	 * @param metadata A set of metadata to be associated with the new diachronic dataset.
	 */
	public String createDiachronicDataset(ArchiveEntityMetadata metadata, String datasetName){
		
		//TODO create with factory
		DiachronicDataset diachronicDataset = new RDFDiachronicDataset();
		HashMap<String, String> metadataMap = metadata.getMetadataMap();
		Set<String> keySet = metadataMap.keySet();
		
		for(String predicate : keySet){
			diachronicDataset.setMetaProperty(predicate, metadataMap.get(predicate));
		}
		DictionaryService dictService = StoreFactory.createDictionaryService();
		String URI = dictService.createDiachronicDataset(diachronicDataset, datasetName);
		return URI;
		
	}

	/**
	 * Updates a diachronic dataset by loading new data to the archive store. 
	 * Accepts an InputStream object that contains the data to be loaded, and 
	 * a URI of the diachronic dataset to be updated. An optional (can be null) 
	 * parameter indicating the serialization format is also available
	 * 
	 * @param input The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @param format The serialization format of the dataset provided by the input stream
	 * 					For RDF data, allowed values are "RDF/XML" (default), "N-triples" and "Turtle".
	 * @throws Exception 
	 */
	public String loadData(InputStream stream, String diachronicDatasetURI, String format) throws Exception {
		//This uploads directly the rdf file defined in the FileInputStream into namedGraph
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(diachronicDatasetURI) != null) {
			return StoreFactory.createDataLoader()
					.loadData(stream, diachronicDatasetURI, format);
		} else
			throw new Exception("Non-existing Diachronic Dataset");
		
	}
	
	/**
	 * Updates a diachronic dataset by loading new data to a Virtuoso instance associated with the archive store. 
	 * Accepts an InputStream object that contains the data to be loaded, and 
	 * a URI of the diachronic dataset to be updated.
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @throws Exception 
	 */
	public String loadData(InputStream stream, String diachronicDatasetURI) throws Exception {
		//This uploads directly the rdf file defined in the FileInputStream into namedGraph
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(diachronicDatasetURI) != null) {
			return StoreFactory.createDataLoader()
					.loadData(stream, diachronicDatasetURI);
		} else
			throw new Exception("Non-existing Diachronic Dataset");
		
	}
	
	/**
	 * Implementation of  {@link org.athena.imis.diachron.archive.api.DataStatement#loadDiachronicDatasetMetadata} 
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @throws Exception when the Diachronic Dataset doesn't exist in the archive
	 */
	public void loadDiachronicDatasetMetadata(InputStream stream, String diachronicDatasetURI) throws Exception {
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDiachronicDataset(diachronicDatasetURI) != null) 
			StoreFactory.createDataLoader().loadMetadata(stream, diachronicDatasetURI);
		else
			throw new Exception("Non-existing Diachronic Dataset");
	}

	/**
	 * Implementation of  {@link org.athena.imis.diachron.archive.api.DataStatement#loadDatasetMetadata} 
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @throws Exception when the Dataset Instatiation URI doesn't exist in the archive
	 */
	public void loadDatasetMetadata(InputStream stream, String datasetURI) throws Exception {
		DictionaryService dict = StoreFactory.createDictionaryService();
		if (dict.getDataset(datasetURI) != null) 
			StoreFactory.createDataLoader().loadMetadata(stream, datasetURI);
		else
			throw new Exception("Non-existing Dataset Instatiation");
	}

}
