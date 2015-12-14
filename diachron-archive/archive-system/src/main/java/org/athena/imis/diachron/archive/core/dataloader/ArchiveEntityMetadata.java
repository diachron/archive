package org.athena.imis.diachron.archive.core.dataloader;

import java.util.HashMap;

/**
 * 
 * Implementation of the Metadata interface for storing metadata information for diachron entities.
 *
 */
public class ArchiveEntityMetadata implements Metadata {

	private String uri;
	private HashMap<String, String> metadataMap;
	
	/**
	 * Sets a metadata map for this ArchiveEntityMetadata object. 
	 * @param metadataMap A HashMap<String, String> containing property-value pairs for this ArchiveEntityMetadata object.
	 */
	public void setMetadataMap(HashMap<String, String> metadataMap){
		this.metadataMap = metadataMap;
	}
	
	/**
	 * Fetches the metadata map associated with this ArchiveEntityMetadata object.
	 * @return A HashMap<String, String> containing property-value pairs of metadata.
	 */
	public HashMap<String, String> getMetadataMap(){
		return this.metadataMap;
	}
	
	/**
	 * Sets the URI of the Archive Entity
	 * @param uri A URI of the Archive entity
	 */
	public void setURI(String uri){
		this.uri = uri;
	}
	
	/**
	 * Fetches the URI of the archive entity
	 * @return A String URI of the archive entity.
	 */
	public String getURI(){
		return this.uri;
	}
}
