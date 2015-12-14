package org.athena.imis.diachron.archive.models;

import java.util.Collection;
import java.util.HashMap;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;

/**
 * Class for record sets
 * @author Marios Meimaris
 *
 */
public class RDFRecordSet extends AbstractDiachronEntity  implements RecordSet {

	private HashMap<String, Record> records = new HashMap<String, Record>();
	
	/**
	 * Returns the record set's records
	 * @return A collection containing the records of the record set
	 */
	public Collection<Record> getRecords() {
		return records.values();
	}

	public ArchiveResultSet getRawData() {
	
		return null;
	}
	
	/**
	 * Adds a new record to the record set
	 */
    public void addRecord(Record rec) {
    	records.put(rec.getId(), rec);
    }

}
