package org.athena.imis.diachron.archive.web.services;


import java.io.IOException;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * JSON String serializer class. Overrides the standard serializer inorder to omit quotes from the a String that contains json formated data.
 *
 */
public class JSONStringSerializer extends JsonSerializer<String> {

    
	@Override
	public void serialize(String JSONString, JsonGenerator gen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {
		
		gen.writeRawValue(JSONString);
		
	}
}