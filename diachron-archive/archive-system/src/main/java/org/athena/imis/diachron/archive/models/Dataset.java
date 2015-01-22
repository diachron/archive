package org.athena.imis.diachron.archive.models;

import java.util.Hashtable;
import java.util.Set;

/**
 * An interface for Datasets.
 *
 */
public interface Dataset extends DiachronEntity {
	public RecordSet getRecordSet();	
	public void setRecordSet(RecordSet rs);
	//public void setMetadata(List<String[]> metadataList);
	//public List<String[]> getMetadata();
	
	public Object getMetaProperty(String propertyName);
	public void setMetaProperty(String name, Object value);
	public Set<String> getMetaPropertiesNames();
	
	public void setMetaProperties(Hashtable<String, Object> datasetMetadata);
	public Hashtable<String, Object> getMetaProperties();
}
