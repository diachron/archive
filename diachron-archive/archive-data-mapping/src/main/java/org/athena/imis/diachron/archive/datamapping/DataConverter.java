package org.athena.imis.diachron.archive.datamapping;

import java.io.InputStream;
import java.io.OutputStream;

public interface DataConverter {
	public void convert(InputStream input, OutputStream out, String datasetName);

}
