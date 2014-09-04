package org.athena.imis.diachron.archive.models;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;

/**
 * An interface for Change Sets.
 *
 */
public interface ChangeSet extends DiachronEntity {
	public String getOldDatasetVersionId();
	public String getNewDatasetVersionId();
	public void setOldDatasetVersionId(String id);
	public void setNewDatasetVersionId(String id);
	public ArchiveResultSet getRawData();

}
