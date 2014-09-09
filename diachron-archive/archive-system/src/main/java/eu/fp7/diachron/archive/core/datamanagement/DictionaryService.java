package eu.fp7.diachron.archive.core.datamanagement;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import eu.fp7.diachron.archive.models.Dataset;
import eu.fp7.diachron.archive.models.DiachronicDataset;

/**
 * 
 * Interface for the implementation of dictionary service classes, such as RDFDictionary and DictionaryCache.
 *
 */
public interface DictionaryService {
	public void storeDiachronicDataset(DiachronicDataset dds) throws IOException;
	String createDiachronicDatasetId() throws IOException;
	public List<DiachronicDataset> getListOfDiachronicDatasets() throws IOException;
	public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDatasetId) throws IOException;
	public Hashtable<String, Object>  getDiachronicDatasetMetadata(String diachronicDatasetId) throws IOException;
	public DiachronicDataset getDiachronicDataset(String id) throws IOException;
		
}
