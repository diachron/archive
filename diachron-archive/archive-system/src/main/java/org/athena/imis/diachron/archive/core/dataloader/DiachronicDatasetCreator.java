package org.athena.imis.diachron.archive.core.dataloader;

import java.io.IOException;

/**
 * 
 * An interface for implementation of Data Statements, i.e. statements that perform write procedures on 
 * the archive store.
 *
 */
public interface DiachronicDatasetCreator {
	
	/**
	 * Creates a new diachronic dataset in the arhive and associates it with metadata.
	 * 
	 * @param metadata The metadata to be associated with the archive.
	 * @return A string URI of the created diachronic dataset. 
	 * @throws IOException
	 */
	public String createDiachronicDataset(ArchiveEntityMetadata metadata) throws IOException;

}
