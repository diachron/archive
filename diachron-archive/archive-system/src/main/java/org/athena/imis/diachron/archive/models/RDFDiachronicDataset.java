package org.athena.imis.diachron.archive.models;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

/**
 * This class implements the DiachronicDataset interface for RDF Diachronic Datasets.
 *
 */
public class RDFDiachronicDataset extends AbstractDiachronEntity implements DiachronicDataset {
	private Hashtable<String, Object> metaProperties = new Hashtable<String, Object>();
	private LinkedHashMap<String, Dataset> datasets = new LinkedHashMap<String, Dataset>();
	private LinkedHashMap<String, ChangeSet> changeSets = new LinkedHashMap<String, ChangeSet>();
	
	/**
	 * Fetches the Metadata Property based on the provided property name.
	 * @param propertyName
	 * @return The requested metadata property
	 */
	public Object getMetaProperty(String propertyName) {
		return metaProperties.get(propertyName);
	}
	
	/**
	 * Sets the Metadata Property based on the name-value pair provided.
	 * @param name
	 * @param value
	 */
	public void setMetaProperty(String name, Object value) {
		this.metaProperties.put(name, value);
	}
	
	/**
	 * Fetches the metadata property names that exist for this diachronic dataset.
	 * @return A Set of metadata property names as Strings.
	 */
	public Set<String> getMetaPropertiesNames() {
		return this.metaProperties.keySet();
	}

	/**
	 * Fetches the instantiations defined for this Diachronic Dataset.
	 * @return A List object containing Dataset objects that are instantiations of this Diachronic Dataset.
	 */
	public List<Dataset> getDatasetInstatiations() {
		return new Vector<Dataset>(datasets.values());
	}

	/**
	 * Fetches the Changes Sets defined for this Diachronic Dataset.
	 * @return A List object containing ChangeSet objects of this Diachronic Dataset.
	 */
	public List<ChangeSet> getChangeSets() {
		return new Vector<ChangeSet>(changeSets.values());
	}

	/**
	 * Adds a new dataset instantiation to this Diachronic Dataset.
	 * @param dataset The dataset instantiation to be added.
	 */
	public void addDatasetInstatiation(Dataset dataset) {		
		datasets.put(dataset.getId(), dataset);
	}

	/**
	 * Adds a new change set to this Diachronic Dataset.
	 * @param changeSet The change set to be added.
	 */
	public void addChangeSet(ChangeSet changeSet) {
		changeSets.put(changeSet.getId(), changeSet);
	}

	/**
	 * Fetches the instantiation defined in the input parameter.
	 * @param id The URI of the dataset instantiation to be fetched.
	 */
	public Dataset getInstatiation(String id) {
		return datasets.get(id);
	}

	/**
	 * Fetches the change set defined in the input parameter.
	 * @param id The URI of the change set to be fetched.
	 */
	public ChangeSet getChangeSet(String id) {
		return changeSets.get(id);
	}

	@Override
	public void setMetaProperties(
			Hashtable<String, Object> diachronicDatasetMetadata) {
		this.metaProperties = diachronicDatasetMetadata;
		
	}

	@Override
	public Hashtable<String, Object> getMetaProperties() {
		return metaProperties;
	}
	
}
