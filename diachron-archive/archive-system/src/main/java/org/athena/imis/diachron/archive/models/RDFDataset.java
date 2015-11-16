package org.athena.imis.diachron.archive.models;

import java.util.Hashtable;
import java.util.List;
import java.util.Set;

/**
 * This class implements Dataset for RDF Dataset objects.
 *
 */
public class RDFDataset extends AbstractDiachronEntity implements Dataset  {
	
	private RecordSet recSet;
	private String changeSetOld;
	private String changeSetNew;
	private boolean fullyMaterialized;
	private String lastFullyMaterialized;
	private List<String> changeSets;
	private String addedGraphId;
	private String deletedGraphId;
	private DiachronicDataset diachronicDataset;
	//private List<String[]> metadataList;

	private Hashtable<String, Object> metaProperties = new Hashtable<String, Object>();
	
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
	 * Fetches the metadata property names that exist for this dataset.
	 * @return A Set of metadata property names as Strings.
	 */
	public Set<String> getMetaPropertiesNames() {
		return this.metaProperties.keySet();
	}
	
	/**
	 * Fetches the record set of this RDFDataset.
	 * @return the recSet
	 */
	public RecordSet getRecordSet() {
		return recSet;
	}

	/**
	 * Sets the record set of this RDFDataset.
	 * @param recSet the recSet to set
	 */
	public void setRecordSet(RecordSet recSet) {
		this.recSet = recSet;
	}


	/**
	 * Sets metadata properties for the dataset.
	 * @param datasetMetadata The dataset metadata.
	 */
	public void setMetaProperties(
			Hashtable<String, Object> datasetMetadata) {
		this.metaProperties = datasetMetadata;
		
	}

	/**
	 * Returns metadata properties of dataset.
	 * @return A hashtable with the metadata properties as predicate-object pairs.
	 */
	public Hashtable<String, Object> getMetaProperties() {
		return metaProperties;
	}

	
	/**
	 * Returns true if the dataset is fully materialized in the archive, else false.
	 * @return A boolean indicator of full materializaton.
	 */
	@Override
	public boolean isFullyMaterialized() {
		
		return fullyMaterialized;
	}

	/**
	 * Sets the full materialization flag. 
	 */
	@Override
	public void setFullyMaterialized(boolean fullyMaterialized) {		
		
		this.fullyMaterialized = fullyMaterialized;
		
	}
	
	/**
	 * Sets the last fully materialized dataset prior to this.
	 */
	@Override
	public void setLastFullyMaterialized(String uri){
		lastFullyMaterialized = uri;
	}
	
	/**
	 * Returns the URI of the last fully materialized dataset prior to this.
	 * @return The URI as a string of the last fully materialized dataset.
	 */
	@Override
	public String getLastFullyMaterialized(){
		return lastFullyMaterialized;
	}

	/**
	 * Returns a list of changesets between this dataset and the last fully materialized.
	 * @return The list of changesets.
	 */
	@Override
	public List<String> getListOfChangesets() {
		
		return changeSets;
	}

	/**
	 * Sets a list of changesets between this dataset and the last fully materialized.	 
	 */
	@Override
	public void setListOfChangesets(List<String> changeSets) {
		
		this.changeSets = changeSets;
		
	}
	
	/**
	 * If the dataset is not fully materialized and has been reconstructed in the past, this method sets
	 * its added and deleted graphs.
	 */
	@Override
	public void setDeltaGraphs(String addedGraphId, String deletedGraphId){
		
		this.addedGraphId = addedGraphId;
		this.deletedGraphId = deletedGraphId;
		
	}

	/**
	 * If the dataset is not fully materialized and has been reconstructed in the past, this method fetches
	 * its added graph.
	 * @return The URI as a string of the added graph.
	 */
	@Override
	public String getAddedGraphId() {
		
		return this.addedGraphId;
		
	}

	/**
	 * If the dataset is not fully materialized and has been reconstructed in the past, this method fetches
	 * its deleted graph.
	 * @return The URI as a string of the deleted graph.
	 */
	@Override
	public String getDeletedGraphId() {
		
		return this.deletedGraphId;
	}
	
	/**
	 * Sets change set that this dataset is the old dataset of.
	 */
	@Override
	public void setChangeSetOld(String cs){
		
		this.changeSetOld = cs;
		
	}
	
	/**
	 * Sets change set that this dataset is the new dataset of.
	 */
	@Override
	public void setChangeSetNew(String cs){
		
		this.changeSetNew = cs;
		
	}
	
	/**
	 * Fetches change set that this dataset is the old dataset of.
	 * @return A string URI of the change set.
	 */
	@Override
	public String getChangeSetOld(){
		return this.changeSetOld;
	}
	
	/**
	 * Fetches change set that this dataset is the new dataset of.
	 * @return A string URI of the change set.
	 */
	@Override
	public String getChangeSetNew(){
		return this.changeSetNew;
	}

	/**
	 * Returns the diachronic dataset that this dataset belongs to.
	 * @return The diachronic dataset object.
	 */
	@Override
	public DiachronicDataset getDiachronicDataset() {
		
		return this.diachronicDataset;
	}

	/**
	 * Sets the diachronic dataset that this dataset belongs to.	
	 */
	@Override
	public void setDiachronicDataset(DiachronicDataset diachronicDataset) {
		
		this.diachronicDataset = diachronicDataset;
		
	}

	
}
