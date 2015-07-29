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


	public void setMetaProperties(
			Hashtable<String, Object> datasetMetadata) {
		this.metaProperties = datasetMetadata;
		
	}

	public Hashtable<String, Object> getMetaProperties() {
		return metaProperties;
	}
	/*
	public void setMetadata(List<String[]> metadataList){
		this.metadataList = metadataList;
	}
	
	public List<String[]> getMetadata(){
		return this.metadataList;
	}
*/

	@Override
	public boolean isFullyMaterialized() {
		
		return fullyMaterialized;
	}

	@Override
	public void setFullyMaterialized(boolean fullyMaterialized) {		
		
		this.fullyMaterialized = fullyMaterialized;
		
	}
	
	@Override
	public void setLastFullyMaterialized(String uri){
		lastFullyMaterialized = uri;
	}
	
	@Override
	public String getLastFullyMaterialized(){
		return lastFullyMaterialized;
	}

	@Override
	public List<String> getListOfChangesets() {
		
		return changeSets;
	}

	@Override
	public void setListOfChangesets(List<String> changeSets) {
		
		this.changeSets = changeSets;
		
	}
	
	@Override
	public void setDeltaGraphs(String addedGraphId, String deletedGraphId){
		
		this.addedGraphId = addedGraphId;
		this.deletedGraphId = deletedGraphId;
		
	}

	@Override
	public String getAddedGraphId() {
		
		return this.addedGraphId;
		
	}

	@Override
	public String getDeletedGraphId() {
		
		return this.deletedGraphId;
	}
	
	@Override
	public void setChangeSetOld(String cs){
		
		this.changeSetOld = cs;
		
	}
	
	@Override
	public void setChangeSetNew(String cs){
		
		this.changeSetNew = cs;
		
	}
	
	@Override
	public String getChangeSetOld(){
		return this.changeSetOld;
	}
	
	@Override
	public String getChangeSetNew(){
		return this.changeSetNew;
	}

	@Override
	public DiachronicDataset getDiachronicDataset() {
		
		return this.diachronicDataset;
	}

	@Override
	public void setDiachronicDataset(DiachronicDataset diachronicDataset) {
		
		this.diachronicDataset = diachronicDataset;
		
	}

	
}
