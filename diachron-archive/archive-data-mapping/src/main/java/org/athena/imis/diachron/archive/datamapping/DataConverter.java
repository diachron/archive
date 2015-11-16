package org.athena.imis.diachron.archive.datamapping;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * This is the interface for classes that implement conversion to the DIACHRON model.  
 * @author Marios Meimaris
 *
 */
public interface DataConverter {
	public void convert(InputStream input, OutputStream out, String datasetName);

}
