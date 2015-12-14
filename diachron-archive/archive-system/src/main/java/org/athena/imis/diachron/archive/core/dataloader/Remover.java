package org.athena.imis.diachron.archive.core.dataloader;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronicDataset;

/**
 * Interface for classes that implement dataset deletion and removal from the archive.
 * @author Marios Meimaris
 *
 */
public interface Remover {
	
	/**
	 * Removes a dataset instantiation
	 * @param dataset The Dataset object to be removed.
	 */
	public void removeDataset(Dataset dataset);

	/**
	 * Removes a diachronic dataset and all of its instantiations
	 * @param diachronicDataset The DiachronicDataset object to be removed.
	 */
	public void removeDiachronicDataset(DiachronicDataset diachronicDataset);
}
