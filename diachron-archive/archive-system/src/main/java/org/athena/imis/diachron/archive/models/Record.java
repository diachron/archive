package org.athena.imis.diachron.archive.models;

import java.util.Collection;

public interface Record extends DiachronEntity {
	public String getSubject();
	public void setSubject(String subject);
	public void addRecordAttribute(RecordAttribute recAttr);
	public Collection<RecordAttribute> getRecordAttributes();

}
