package org.athena.imis.diachron.archive.models;

/**
 * Class for record attributes
 * @author Marios Meimaris
 *
 */
public class RDFRecordAttribute extends AbstractDiachronEntity implements RecordAttribute {
	
	private String property;
	private String propertyValue;
	private boolean propertyValueIsLiteral = false;
	
	/**
	 * Returns the attribute property
	 * @return A URI as string of the property
	 */
	public String getProperty() {
		return property;
	}
	
	/**
	 * Sets the attribute property	 
	 */
	public void setProperty(String property) {
		this.property = property;
	}
	
	/**
	 * Returns the attribute object
	 * @return A URI as string of the object
	 */
	public String getPropertyValue() {
		return propertyValue;
	}
	
	/**
	 * Sets the attribute object
	 * @return A URI as string of the object
	 */
	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}
	
	/**
	 * Sets a flag to indicate if the object is a literal
	 */
	public void setPropertyValueIsLiteral(){
		this.propertyValueIsLiteral = true;
	}
	
	/**
	 * Returns the flag of the literal object
	 * @return True if object is literal, else false.
	 */
	public boolean getPropertyValueIsLiteral(){
		return this.propertyValueIsLiteral;
	}
	

}
