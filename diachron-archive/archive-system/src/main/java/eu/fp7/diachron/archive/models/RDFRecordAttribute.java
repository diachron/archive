package eu.fp7.diachron.archive.models;

public class RDFRecordAttribute extends AbstractDiachronEntity implements RecordAttribute {
	private String property;
	private String propertyValue;
	private boolean propertyValueIsLiteral = false;
    
    public RDFRecordAttribute(String id) {
      super(id);
    }
    
	public String getProperty() {
		return property;
	}
	public void setProperty(String property) {
		this.property = property;
	}
	public String getPropertyValue() {
		return propertyValue;
	}
	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}
	
	public void setPropertyValueIsLiteral(){
		this.propertyValueIsLiteral = true;
	}
	
	public boolean getPropertyValueIsLiteral(){
		return this.propertyValueIsLiteral;
	}
	

}
