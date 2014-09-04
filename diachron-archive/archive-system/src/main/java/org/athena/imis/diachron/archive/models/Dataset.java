package org.athena.imis.diachron.archive.models;

/**
 * An interface for Datasets.
 *
 */
public interface Dataset extends DiachronEntity {
	public RecordSet getRecordSet();
	public void setRecordSet(RecordSet rs);
	
}
