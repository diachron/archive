package org.athena.imis.diachron.archive.core.dataloader;

import java.util.Hashtable;
import java.util.List;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronicDataset;

/**
 * 
 * Interface for the implementation of dictionary service classes, such as RDFDictionary and DictionaryCache.
 *
 */
public interface DictionaryService {
	public String createDiachronicDataset(DiachronicDataset dds);
	String createDiachronicDatasetId();
	public List<DiachronicDataset> getListOfDiachronicDatasets();
	public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDatasetId);
	public Hashtable<String, Object>  getDiachronicDatasetMetadata(String diachronicDatasetId);
	public DiachronicDataset getDiachronicDataset(String id);
		
}
