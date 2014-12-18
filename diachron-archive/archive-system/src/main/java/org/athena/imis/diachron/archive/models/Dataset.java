package org.athena.imis.diachron.archive.models;

import java.util.List;

/**
 * An interface for Datasets.
 *
 */
public interface Dataset extends DiachronEntity {
	public RecordSet getRecordSet();	
	public void setRecordSet(RecordSet rs);
	public void setMetadata(List<String[]> metadataList);
	public List<String[]> getMetadata();
}
