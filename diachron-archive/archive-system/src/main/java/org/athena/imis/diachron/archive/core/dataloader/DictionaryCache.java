package org.athena.imis.diachron.archive.core.dataloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * The cache that stores the Archive's dataset dictionary information for fast retrieval. 
 *
 */
public class DictionaryCache implements DictionaryService {
	private static final Hashtable<String, DiachronicDataset> diachronicDatasets 
		= new Hashtable<String, DiachronicDataset>();

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
	public String createDiachronicDataset(DiachronicDataset dds) {
		String id = persistentStorage.createDiachronicDataset(dds);
		dds.setId(id);
		diachronicDatasets.put(id, dds);
		return id;
		
	}

	public String createDiachronicDatasetId() {
		return persistentStorage.createDiachronicDatasetId();
	}

	/**
	 * Gets a list of diachronic datasets from the cache.
	 * @return A List<DiachronicDataset> from the cache.
	 */
	@Override
	public List<DiachronicDataset> getListOfDiachronicDatasets() {
		return new ArrayList<DiachronicDataset>(diachronicDatasets.values());
	}

	/**
	 * Gets a list of dataset versions for the diachronic dataset defined in the input parameter.
	 * 
	 * @param diachronicDataset The DiachronicDataset object whose versions are to be returned.
	 * @return A List of Dataset objects.
	 */
	@Override
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
	@Override
	public Hashtable<String, Object> getDiachronicDatasetMetadata(
			String diachronicDatasetId) {
		return getDiachronicDatasetMetadata(diachronicDatasetId);
	}


	/**
	 * Returns a DiachronicDataset object based on the URI defined in the input parameter.
	 * @param id The URI of the diachronic dataset to be returned.
	 * @return A DiachronicDataset object.
	 */
	@Override
	public DiachronicDataset getDiachronicDataset(String id) {
		return diachronicDatasets.get(id);
	}
	
	

}
