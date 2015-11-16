package org.athena.imis.diachron.archive.core.dataloader;

import java.io.InputStream;

import com.hp.hpl.jena.rdf.model.Model;
/**
 * Provides the main functionality for loading data to the archive
 * 
 */
public interface Loader {

	/**
	 * 
	 * @param model
	 * @param namedGraph
	 */
	public void loadModel(Model model, String namedGraph);

	/**
	 * Loads a new dataset to an existing diachronic dataset. 
	 * @param stream
	 * @param diachronicDatasetURI
	 * @throws Exception 
	 */
	public String loadData(InputStream stream,
			String diachronicDatasetURI) throws Exception;
	
	/**
	 * Loads a new dataset to an existing diachronic dataset, based on the specified format. 
	 * @param stream
	 * @param diachronicDatasetURI
	 * @param format The serialization format of the incoming dataset.
	 * @throws Exception 
	 */
	public String loadData(InputStream stream,
			String diachronicDatasetURI, String format) throws Exception;
	
	/**
	 * Loads a new dataset to an existing diachronic dataset, based on the specified format and a version number. 
	 * @param stream
	 * @param diachronicDatasetURI
	 * @param format The serialization format of the incoming dataset.
	 * @param versionNumber The version number to be associated with the new dataset.
	 * @throws Exception 
	 */
	public String loadData(InputStream stream,
			String diachronicDatasetURI, String format, String versionNumber) throws Exception;
	
	/**
	 * 
	 * @param stream
	 * @param diachronicDatasetURI
	 */
	public void loadMetadata(InputStream stream,
			String diachronicDatasetURI);

}