package org.athena.imis.diachron.archive.models;

import java.util.Collection;
import java.util.HashMap;

/**
 * Class for DIACHRON records
 * @author Marios Meimaris
 *
 */
public class RDFRecord extends AbstractDiachronEntity implements Record {
	
	private String subject;
	private HashMap<String, RecordAttribute> attributes = new HashMap<String, RecordAttribute>();
	
	/**
	 * Returns the record's subject.
	 * @return The subject URI as a string
	 */
	public String getSubject() {
		return subject;
	}

	/**
	 * Sets the record's subject.
	 */
	public void setSubject(String subject) {
		this.subject = subject;
	}

	/**
	 * Adds a record attribute.
	 * @param recAttr The record attribute to be added.
	 */
	public void addRecordAttribute(RecordAttribute recAttr) {
		attributes.put(recAttr.getId(), recAttr);
	}

	/**
	 * Returns the record attributes of the record.
	 * @return A collection of record attributes
	 */
	public Collection<RecordAttribute> getRecordAttributes() {
		return attributes.values();
	}

}
