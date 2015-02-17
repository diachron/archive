package org.athena.imis.diachron.archive.models;

import java.net.URI;

import org.athena.imis.diachron.archive.utils.URIUtils;

/**
 * @author Simon Jupp
 * @date 10/02/2014
 * Functional Genomics Group EMBL-EBI
 */
public class DiachronURIFactory {

	private String datasetName;
	private String version; 
	
	public DiachronURIFactory(String datasetName, String version) {
		this.datasetName = datasetName;
		this.version = version;
	}

    public URI generateRecordUri(URI subject) {
        String recordId = URIUtils.generateHashEncodedID(subject.toString(), datasetName);//, version);
        return URI.create(DiachronOntology.diachronResourcePrefix+ "record/" + datasetName + /*"/" + version + */"/" + recordId );
    }

    public URI generateRecordAttributeUri(URI subject, URI predicate, URI object) {
        String attributeId = URIUtils.generateHashEncodedID(subject.toString(), predicate.toString(), object.toString());//, version);
        return URI.create(DiachronOntology.diachronResourcePrefix + "attribute/" + datasetName + /*"/" + version + */"/" + attributeId );
    }

    public URI generateRecordAttributeUri(URI subject, URI predicate, String object) {
        String attributeId = URIUtils.generateHashEncodedID(subject.toString(), predicate.toString(), object);//, version);
        return URI.create(DiachronOntology.diachronResourcePrefix + "attribute/" + datasetName + /*"/" + version + */"/" + attributeId );
    }

    public URI generateDatasetUri() {
        String recordId = URIUtils.generateHashEncodedID(datasetName, version);
        return URI.create(DiachronOntology.diachronResourcePrefix + "dataset/" + datasetName + "/" + version + "/" + recordId );
    }
    
    public URI generateDiachronicDatasetUri() {
        String recordId = URIUtils.generateHashEncodedID(datasetName);
        return URI.create(DiachronOntology.diachronResourcePrefix + "diachronicDataset/" + datasetName + "/" + recordId );
    }

    public URI generateDiachronRecordSetURI() {
        String recordId = URIUtils.generateHashEncodedID(datasetName, version);
        return URI.create(DiachronOntology.diachronResourcePrefix + "recordset/" + datasetName + "/" + version + "/" + recordId );
    }
    public URI generateDiachronSchemaSetURI() {
        String recordId = URIUtils.generateHashEncodedID(datasetName, version);
        return URI.create(DiachronOntology.diachronResourcePrefix + "schemaset/" + datasetName + "/" + version + "/" + recordId );
    }
}

