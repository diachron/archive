package org.athena.imis.diachron.archive.core.dataloader;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.RDFDataset;

import com.hp.hpl.jena.graph.Graph;

/**
 * 
 * Interface for the implementation of dictionary service classes, such as RDFDictionary and DictionaryCache.
 *
 */
public interface DictionaryService {
	public String createDiachronicDataset(DiachronicDataset dds, String datasetName) throws Exception;
	String createDiachronicDatasetId(String datasetName);
	public List<DiachronicDataset> getListOfDiachronicDatasets();
	public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDatasetId);
	public Hashtable<String, Object>  getDiachronicDatasetMetadata(String diachronicDatasetId);
	public DiachronicDataset getDiachronicDataset(String id);
	public Dataset getDataset(String id);
	public void addDataset(Graph graph, String diachronicDatasetURI, String datasetURI);
	public void addRecordSet(Graph graph, String recordSetURI, String datasetURI);
	public void addDatasetMetadata(Graph graph, ArrayList<RDFDataset> list, String diachronicDatasetURI);
	public void addDatasetMetadata(Graph graph, ArrayList<RDFDataset> list, String diachronicDatasetURI, String versionNumber);
	//public void insertDatasetMetadata(String )
		
}
