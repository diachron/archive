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
	 * 
	 * @param stream
	 * @param diachronicDatasetURI
	 */
	public void loadData(InputStream stream, String diachronicDatasetURI) throws IOException;

}