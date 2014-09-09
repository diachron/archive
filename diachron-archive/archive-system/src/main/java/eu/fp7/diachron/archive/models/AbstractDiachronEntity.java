package eu.fp7.diachron.archive.models;

/**
 * 
 * The top class of Diachron Entities.
 *
 */
public class AbstractDiachronEntity implements DiachronEntity {

	private final String id;
	
	public AbstractDiachronEntity(String id) {
	  this.id = id;
	}
	
	/**
	 * Fetches the id of the Diachron Entity.
	 * @return The String id of the Diachron Entity.
	 */
	public String getId() {
		return id;
	}

}
