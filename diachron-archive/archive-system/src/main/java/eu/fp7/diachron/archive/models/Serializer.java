package eu.fp7.diachron.archive.models;

import java.util.List;

public interface Serializer {
	public String serialize(List<? extends DiachronEntity> list) throws Exception;
		

}
