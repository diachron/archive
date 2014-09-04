package org.athena.imis.diachron.archive.models;

import java.util.List;

import org.athena.imis.diachron.archive.api.ArchiveResultSet;

public interface Serializer {
	public String serialize(List<? extends DiachronEntity> list) throws Exception;
		

}
