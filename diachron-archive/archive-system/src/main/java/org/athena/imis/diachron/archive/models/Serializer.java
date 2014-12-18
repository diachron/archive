package org.athena.imis.diachron.archive.models;

import java.util.List;

public interface Serializer {
	public String serialize(List<? extends DiachronEntity> list) throws Exception;
		

}
