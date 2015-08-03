package org.athena.imis.diachron.archive.models;

import java.util.Collection;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;

/**
 * Interface for DIACHRON record sets
 * @author Marios Meimaris
 *
 */
public interface RecordSet extends DiachronEntity {
	public ArchiveResultSet getRawData();
	public Collection<Record> getRecords();
	public void addRecord(Record record);
}
