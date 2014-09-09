package eu.fp7.diachron.archive.models;

public interface RecordAttribute extends DiachronEntity  {
	public String getProperty();
	public void setProperty(String property);
	public String getPropertyValue();
	public void setPropertyValue(String value);
	public void setPropertyValueIsLiteral();
	public boolean getPropertyValueIsLiteral();
}
