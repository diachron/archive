package eu.fp7.diachron.archive.models;

import com.hp.hpl.jena.query.QueryExecution;

/**
 * An interface for Change Sets.
 *
 */
public interface ChangeSet extends DiachronEntity {
	public String getOldDatasetVersionId();
	public String getNewDatasetVersionId();
	public void setOldDatasetVersionId(String id);
	public void setNewDatasetVersionId(String id);
	public QueryExecution getRawData();

}
