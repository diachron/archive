package org.athena.imis.diachron.archive.models;

import java.util.List;

/**
 * Interface for the dataset serializer
 * @author Marios Meimaris
 *
 */
public interface Serializer {
	public String serialize(List<? extends DiachronEntity> list) throws Exception;
		

}
