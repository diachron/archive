package org.athena.imis.diachron.archive.models;

import java.util.Collection;

/**
 * Interface for DIACHRON records.
 * @author Marios Meimaris
 *
 */
public interface Record extends DiachronEntity {
	public String getSubject();
	public void setSubject(String subject);
	public void addRecordAttribute(RecordAttribute recAttr);
	public Collection<RecordAttribute> getRecordAttributes();

}
