package org.athena.imis.diachron.archive.models;

import java.util.Hashtable;
import java.util.List;
import java.util.Set;

/**
 * An interface for Datasets.
 *
 */
public interface Dataset extends DiachronEntity {
	public RecordSet getRecordSet();	
	public void setRecordSet(RecordSet rs);
	public DiachronicDataset getDiachronicDataset();
	public void setDiachronicDataset(DiachronicDataset diachronicDataset);
	//public void setMetadata(List<String[]> metadataList);
	//public List<String[]> getMetadata();
	
	public Object getMetaProperty(String propertyName);
	public void setMetaProperty(String name, Object value);
	public Set<String> getMetaPropertiesNames();
	
	public void setMetaProperties(Hashtable<String, Object> datasetMetadata);
	public Hashtable<String, Object> getMetaProperties();
	
	public boolean isFullyMaterialized();
	public void setFullyMaterialized(boolean isfm);
	public void setLastFullyMaterialized(String uri);
	public String getLastFullyMaterialized();
	public List<String> getListOfChangesets();
	public void setListOfChangesets(List<String> changeSets);
	public void setChangeSetOld(String changeSet);
	public void setChangeSetNew(String changeSet);
	public String getChangeSetOld();
	public String getChangeSetNew();
	public void setDeltaGraphs(String addedGraphId, String deletedGraphId);
	public String getAddedGraphId();
	public String getDeletedGraphId();
}
