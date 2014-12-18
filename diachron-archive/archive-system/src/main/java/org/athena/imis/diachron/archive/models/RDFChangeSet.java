package org.athena.imis.diachron.archive.models;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;

/**
 * 
 * This class representes RDF change sets, an implementation of the ChangeSet interface.
 *
 */
public class RDFChangeSet  extends AbstractDiachronEntity implements ChangeSet {
	private String oldDatasetVersionId;
	private String newDatasetVersionId;
	
	/**
	 * Fetches the old version of the change set.
	 * @return The URI of the old version of the change set.
	 */
	public String getOldDatasetVersionId() {
		return oldDatasetVersionId;
	}
	
	/**
	 * Sets the old version of the change set.
	 * @param oldDatasetVersionId The URI of the old version of the change set.
	 */
	public void setOldDatasetVersionId(String oldDatasetVersionId) {
		this.oldDatasetVersionId = oldDatasetVersionId;
	}
	
	/**
	 * Fetches the new version of the change set.
	 * @return The URI of the new version of the change set.
	 */
	public String getNewDatasetVersionId() {
		return newDatasetVersionId;
	}
	
	/**
	 * Sets the new version of the change set.
	 * @param newDatasetVersionId The URI of the new version of the change set.
	 */
	public void setNewDatasetVersionId(String newDatasetVersionId) {
		this.newDatasetVersionId = newDatasetVersionId;
	}
	
	public ArchiveResultSet getRawData() {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	
	

}
