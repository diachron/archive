package org.athena.imis.diachron.archive.models;

import java.util.Collection;
import java.util.HashMap;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;


public class RDFRecordSet extends AbstractDiachronEntity  implements RecordSet {

	private HashMap<String, Record> records = new HashMap<String, Record>();
	
	public Collection<Record> getRecords() {
		return records.values();
	}
	@Override
	public ArchiveResultSet getRawData() {
		// TODO Auto-generated method stub
		return null;
	}
	
    public void addRecord(Record rec) {
    	records.put(rec.getId(), rec);
    }

}
