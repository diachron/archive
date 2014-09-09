package org.athena.imis.diachron.archive.core.dataloader;

import java.io.IOException;
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
	public void loadModel(Model model, String namedGraph) throws IOException;

	/**
     * Updates a diachronic dataset by loading new data to the archive store. 
     * Accepts an InputStream object that contains the data to be loaded, and 
     * a URI of the diachronic dataset to be updated.
     * 
     * @param stream The {@link InputStream} that contains the data to be loaded.
     * @param diachronicDatasetURI The URI of the diachronic dataset to write to.
     */
	public void loadData(InputStream stream, String diachronicDatasetURI) throws IOException;

}