package org.athena.imis.diachron.archive.core.dataloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.RDFDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;

/**
 * 
 * The cache that stores the Archive's dataset dictionary information for fast retrieval. 
 *
 */
public class DictionaryCache implements DictionaryService {
	
	// diachronic dataset cache
	private static final Hashtable<String, DiachronicDataset> diachronicDatasets 
		= new Hashtable<String, DiachronicDataset>();

	// flat cache of all datasets instantiations for easy access
	private static final Hashtable<String, Dataset> datasetInstantiations 
		= new Hashtable<String, Dataset>();

	private static final Logger logger = LoggerFactory.getLogger(DictionaryCache.class);

	private DictionaryService persistentStorage = null;

	private static boolean initialized = false;
	static {
		logger.info("STATIC");
        if (!initialized) {
        	logger.info("STATIC INITIALIZING");
            init();
		}
	}
	
	
	DictionaryCache(DictionaryService persistentStorage) {
		this.persistentStorage = persistentStorage;
	}
	
	/**
	 * Initializes the cache and populates it with objects from the dictionary of datasets.
	 */
	public static void init() {
		if (!initialized) {
			logger.info("INITIALIZING DICTIONARY");
	        
			DictionaryService store = StoreFactory.createPersDictionaryService();
			Collection<DiachronicDataset> dDatasets = store.getListOfDiachronicDatasets();			
			for(DiachronicDataset dds: dDatasets) {				
				diachronicDatasets.put(dds.getId(), dds);				
				List<Dataset> datasets = store.getListOfDatasets(dds);					
				dds.setMetaProperties(store.getDiachronicDatasetMetadata(dds.getId()));
				for (Dataset ds : datasets) {
					dds.addDatasetInstatiation(ds);
					datasetInstantiations.put(ds.getId(), ds);
					//dds.setMetaProperties(store.getDiachronicDatasetMetadata(dds.getId()));
				}
				
			}
			
			initialized = true;
			logger.info("DICTIONARY INITIALIZED");
		}
        
	}
	
	/**
	 * Creates a new diachronic dataset entry in the cache.
	 * @param dds The DiachronicDataset entry to be created in the cache.
	 * @return A String with the URI of the Diachronic Dataset.
	 */
	public String createDiachronicDataset(DiachronicDataset dds, String datasetName) {
		String id = persistentStorage.createDiachronicDataset(dds, datasetName);
		dds.setId(id);
		diachronicDatasets.put(id, dds);
		return id;
		
	}

	public String createDiachronicDatasetId(String datasetName) {
		return persistentStorage.createDiachronicDatasetId(datasetName);
	}

	/**
	 * Gets a list of diachronic datasets from the cache.
	 * @return A List<DiachronicDataset> from the cache.
	 */
	public List<DiachronicDataset> getListOfDiachronicDatasets() {
		return new ArrayList<DiachronicDataset>(diachronicDatasets.values());
	}

	/**
	 * Gets a list of dataset versions for the diachronic dataset defined in the input parameter.
	 * 
	 * @param diachronicDataset The DiachronicDataset object whose versions are to be returned.
	 * @return A List of Dataset objects.
	 */
	public List<Dataset> getListOfDatasets(
			DiachronicDataset diachronicDataset) {		
		if (diachronicDatasets.contains(diachronicDataset)) {			
			return diachronicDataset.getDatasetInstatiations();
		} else 			
			return null;		
	}

	/**
	 * Fetches the diachronic dataset metadata of the diachronic dataset defined in the input parameter.
	 * 
	 * @param diachronicDatasetId The diachronic dataset URI whose metadata are to be returned.
	 * @return A Hashtable with the diachronic dataset's metadata. 
	 */
	public Hashtable<String, Object> getDiachronicDatasetMetadata(
			String diachronicDatasetId) {
		return getDiachronicDatasetMetadata(diachronicDatasetId);
	}


	/**
	 * Returns a DiachronicDataset object based on the URI defined in the input parameter.
	 * @param id The URI of the diachronic dataset to be returned.
	 * @return A DiachronicDataset object or null if not found.
	 */
	public DiachronicDataset getDiachronicDataset(String id) {
		return diachronicDatasets.get(id);
	}
	
	/**
	 * Returns a Dataset object based on the URI defined in the input parameter.
	 * @param id The URI of the dataset to be returned.
	 * @return A Dataset object or null if not found.
	 */
	public Dataset getDataset(String id) {
		return datasetInstantiations.get(id);
	}

	@Override
	public void addDataset(Graph graph, String diachronicDatasetURI, String datasetId) {
		persistentStorage.addDataset(graph, diachronicDatasetURI, datasetId);
		Dataset ds = new RDFDataset();			
		ds.setId(datasetId);
		datasetInstantiations.put(datasetId, ds);
		diachronicDatasets.get(diachronicDatasetURI).addDatasetInstatiation(ds);
	}

	@Override
	public void addRecordSet(Graph graph, String recordSetURI, String datasetId) {
		persistentStorage.addRecordSet(graph, recordSetURI, datasetId);
		/*RecordSet ds = new RDFRecordSet();			
		ds.setId(recordSetURI);
		datasetInstantiations.put(datasetId, ds);
		diachronicDatasets.get(diachronicDatasetURI).addDatasetInstatiation(ds);*/
	}
	
	public void addDatasetMetadata(Graph graph, ArrayList<RDFDataset> list, String diachronicDatasetURI){
			persistentStorage.addDatasetMetadata(graph, list, diachronicDatasetURI);
			for(RDFDataset dataset : list){
				datasetInstantiations.put(dataset.getId(), dataset);
				diachronicDatasets.get(diachronicDatasetURI).addDatasetInstatiation(dataset);
			}
	}

}
