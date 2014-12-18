package org.athena.imis.diachron.archive.models;

import java.util.Collection;
import java.util.HashMap;

public class RDFRecord extends AbstractDiachronEntity implements Record {
	private String subject;
	private HashMap<String, RecordAttribute> attributes = new HashMap<String, RecordAttribute>();
	
	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public void addRecordAttribute(RecordAttribute recAttr) {
		attributes.put(recAttr.getId(), recAttr);
	}

	public Collection<RecordAttribute> getRecordAttributes() {
		return attributes.values();
	}

}
