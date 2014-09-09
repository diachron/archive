package eu.fp7.diachron.archive.models;

import java.util.Collection;

import com.hp.hpl.jena.query.QueryExecution;

public interface RecordSet extends DiachronEntity {
	public QueryExecution getRawData();
	public Collection<Record> getRecords();
	public void addRecord(Record record);
}
