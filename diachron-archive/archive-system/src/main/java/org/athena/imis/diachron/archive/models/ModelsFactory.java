package org.athena.imis.diachron.archive.models;

/**
 * 
 * This class is a factory for the creation of Diachron Entities, such as Diachronic Datasets and Datasets.
 *
 */
public class ModelsFactory {

	/**
	 * Creates a new DiachronicDataset object.
	 * @return The created DiachronicDataset object.
	 */
	public static DiachronicDataset createDiachronicDataset() {
		return new RDFDiachronicDataset();
	}

	/**
	 * Creates a new Dataset object and associates it with the DiachronicDataset provided in the input parameter.
	 * @param dds DiachronicDataset to be associated with the created Dataset object.
	 * @return The created Dataset object.
	 */
	public static Dataset createDataset(DiachronicDataset dds) {
		Dataset ds =  new RDFDataset();
		dds.addDatasetInstatiation(ds);
		return ds;
	}

	/**
	 * Creates an RDFSerializer.
	 * @return The created RDFSerializer object.
	 */
	public static Serializer getSerializer() {
		// TODO Auto-generated method stub
		return new RDFSerializer();
	}
	
	public static Record createRecord() {
		return new RDFRecord();
	}
	
	public static RecordAttribute createRecordAttribute() {
		return new RDFRecordAttribute();
	}
}
