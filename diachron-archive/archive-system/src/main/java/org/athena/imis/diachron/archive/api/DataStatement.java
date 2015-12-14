package org.athena.imis.diachron.archive.api;

import java.io.InputStream;

import org.athena.imis.diachron.archive.core.dataloader.ArchiveEntityMetadata;

/**
 * 
 * An interface for implementation of Data Statements, i.e. statements that perform write procedures on 
 * the archive store.
 *
 */
public interface DataStatement {

	/**
	 * Updates a diachronic dataset by loading new data to the archive store. 
	 * Accepts an InputStream object that contains the data to be loaded, and 
	 * a URI of the diachronic dataset to be updated.
	 * 
	 * @param input The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @throws Exception 
	 */
	public String loadData(InputStream input, String diachronicDatasetURI) throws Exception;
	
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
	public String loadData(InputStream input, String diachronicDatasetURI, String format) throws Exception;
	
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
	 * @param versionNumber A representative number for the dataset version.
	 * 
	 * @throws Exception 
	 */
	public String loadData(InputStream stream, String diachronicDatasetURI, String format, String versionNumber) throws Exception;
		
	
	
	/**
	 * Creates a new diachronic dataset in the arhive and associates it with metadata.
	 * 
	 * @param metadata The metadata to be associated with the archive.
	 * @return A string URI of the created diachronic dataset. 
	 * @throws Exception 
	 */
	public String createDiachronicDataset(ArchiveEntityMetadata metadata, String datasetName) throws Exception;

	/**
	 * Attributes metadata to a Diachronic Dataset. 
	 * Accepts an InputStream object that contains the data to be loaded in JSON-LD, and 
	 * a URI of the diachronic dataset to be updated.
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
	 * @throws Exception when the Diachronic Dataset URI doesn't exist in the archive 
	 */
	public void loadDiachronicDatasetMetadata(InputStream stream, String diachronicDatasetURI) throws Exception;

	/**
	 * Attributes metadata to a Diachronic Dataset Instatiation. 
	 * Accepts an InputStream object that contains the data to be loaded in JSON-LD, and 
	 * a URI of the dataset instatiations to be updated.
	 * 
	 * @param stream The InputStream that contains the data to be loaded.
	 * @param datasetURI The URI of the dataset instatiation to write to.
	 * @throws Exception when the Dataset Instatiation URI doesn't exist in the archive
	 */
	public void loadDatasetMetadata(InputStream stream, String datasetURI) throws Exception;
	
	/**
	 * Deletes a dataset instantiation.
	 * @param datasetURI The URI of the dataset instantiation to be removed from the archive.
	 * @throws Exception
	 */
	public void removeDataset(String datasetURI) throws Exception;
}
