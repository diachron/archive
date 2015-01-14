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
	 * 
	 * @param stream
	 * @param diachronicDatasetURI
	 * @throws Exception 
	 */
	public String loadData(InputStream stream,
			String diachronicDatasetURI) throws Exception;
	
	public String loadData(InputStream stream,
			String diachronicDatasetURI, String format) throws Exception;
	
	/**
	 * 
	 * @param stream
	 * @param diachronicDatasetURI
	 */
	public void loadMetadata(InputStream stream,
			String diachronicDatasetURI);

}