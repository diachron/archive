package org.athena.imis.diachron.archive.models;

/**
 * 
 * The top class of Diachron Entities.
 *
 */
public class AbstractDiachronEntity implements DiachronEntity {

	private String id;
	
	/**
	 * Fetches the id of the Diachron Entity.
	 * @return The String id of the Diachron Entity.
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets the id of this Diachron Entity.
	 * @param id The id of the Diachron Entity to be set.
	 */
	public void setId(String id) {
		this.id = id;
	}

}
