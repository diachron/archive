package eu.fp7.diachron.archive.models;

import java.util.Hashtable;
import java.util.List;
import java.util.Set;

/**
 * An interface for Diachronic Datasets.
 *
 */
public interface DiachronicDataset extends DiachronEntity {
	public Object getMetaProperty(String propertyName);
	public void setMetaProperty(String name, Object value);
	public Set<String> getMetaPropertiesNames();
	
	public List<Dataset> getDatasetInstatiations();
	public List<ChangeSet> getChangeSets();
	
	public void addDatasetInstatiation(Dataset dataset);
	public void addChangeSet(ChangeSet changeSet);
	
	public Dataset getInstatiation(String id);
	public ChangeSet getChangeSet(String id);
	public void setMetaProperties(Hashtable<String, Object> diachronicDatasetMetadata);
	public Hashtable<String, Object> getMetaProperties();

}
