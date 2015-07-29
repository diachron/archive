package org.athena.imis.diachron.archive.models;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * 
 * This class contains jena definitions for the ontology/vocabulary terms used in DIACHRON.
 *
 */
public class DiachronOntology {

	public static final String diachronNamespace = "http://www.diachron-fp7.eu/";
	public static final String diachronResourcePrefix = diachronNamespace + "resource/";
	public static final String changesOntologyNamespace = "http://www.diachron-fp7.eu/changes/"; 
	public static final String sparqlResultsNamespace = "http://www.w3.org/2005/sparql-results#";
	public static final String provNamespace = "http://www.w3.org/ns/prov#";
	
	public static final Resource diachronicDataset;
	public static final Resource dataset;
	public static final Resource record;
	public static final Resource recordAttribute;
	public static final Resource set;
	public static final Resource recordSet;
	public static final Resource schemaSet;
	public static final Resource resourceSet;
	public static final Resource dataObject;
	public static final Resource schemaObject;
	public static final Resource changeSet;
	public static final Resource attributeProperty;
	public static final Resource componentProperty;
	public static final Resource dimensionProperty;
	public static final Resource measureProperty;
	public static final Resource ontologicalProperty;
	public static final Resource multidimensionalObservation;
	public static final Resource codeList;
	public static final Resource codeListTerm;
	public static final Resource entity;
	public static final Resource factTable;
	public static final Resource ontologyClass;
	public static final Resource relationalColumn;
	public static final Resource relationalTable;
	public static final Resource resource;
	public static final Resource schemaClass;
	public static final Resource schemaProperty;
	
	
	
	
	
	public static final Property hasInstantiation;
	public static final Property isInstantiationOf;
	public static final Property hasAttribute;
	public static final Property hasAttributeValue;
	public static final Property hasDataObject;
	public static final Property hasDimension;
	public static final Property hasMeasure;
	public static final Property hasOntologyProperty;
	public static final Property hasProperty;
	public static final Property hasRecord;
	public static final Property hasRecordAttribute;
	public static final Property hasRecordSet;
	public static final Property hasTempRecordSet;
	public static final Property hasRelationalColumn;
	public static final Property hasResourceSet;
	public static final Property hasSchemaObject;
	public static final Property hasSchemaSet;
	public static final Property subject;
	public static final Property predicate;
	public static final Property object;
	public static final Property hasDefinition;
	public static final Property codelist;
	public static final Property hierarchy;
	public static final Property inScheme;
	public static final Property oldVersion;
	public static final Property newVersion;
	public static final Property paramValue;
	public static final Property hasFactTable;
	public static final Property isFullyMaterialized;
	public static final Property addedGraph;
	public static final Property deletedGraph;
	public static final Property creationTime;
	
	//SPARQL results
	public static final Property resultVariable;
	public static final Property solution;
	public static final Property binding;
	public static final Property variable;
	public static final Property value;
	
	//PROV
	public static final Property generatedAtTime;
	public static final Property hasChangeSet;
	
	static {
		diachronicDataset = ResourceFactory.createResource(diachronResourcePrefix + "DiachronicDataset");		
		dataset = ResourceFactory.createResource(diachronResourcePrefix + "Dataset");
		record = ResourceFactory.createResource(diachronResourcePrefix + "Record");
		recordAttribute = ResourceFactory.createResource(diachronResourcePrefix + "RecordAttribute");
		set = ResourceFactory.createResource(diachronResourcePrefix + "Set");
		recordSet = ResourceFactory.createResource(diachronResourcePrefix + "RecordSet");
		schemaSet = ResourceFactory.createResource(diachronResourcePrefix + "SchemaSet");
		resourceSet = ResourceFactory.createResource(diachronResourcePrefix + "ResourceSet");
		dataObject = ResourceFactory.createResource(diachronResourcePrefix + "DataObject");
		schemaObject = ResourceFactory.createResource(diachronResourcePrefix + "SchemaObject");
		changeSet = ResourceFactory.createResource(diachronResourcePrefix + "ChangeSet");
		attributeProperty = ResourceFactory.createResource(diachronResourcePrefix + "AttributeProperty");
		componentProperty = ResourceFactory.createResource(diachronResourcePrefix + "ComponentProperty");
		dimensionProperty = ResourceFactory.createResource(diachronResourcePrefix + "DimensionProperty");
		measureProperty = ResourceFactory.createResource(diachronResourcePrefix + "MeasureProperty");
		ontologicalProperty = ResourceFactory.createResource(diachronResourcePrefix + "OntologicalProperty");
		multidimensionalObservation = ResourceFactory.createResource(diachronResourcePrefix + "Observation");
		codeList = ResourceFactory.createResource(diachronResourcePrefix + "CodeList");
		codeListTerm = ResourceFactory.createResource(diachronResourcePrefix + "CodeListTerm");
		entity = ResourceFactory.createResource(diachronResourcePrefix + "Entity");
		factTable = ResourceFactory.createResource(diachronResourcePrefix + "FactTable");
		ontologyClass = ResourceFactory.createResource(diachronResourcePrefix + "OntologyClass");
		relationalColumn = ResourceFactory.createResource(diachronResourcePrefix + "RelationalColumn");
		relationalTable = ResourceFactory.createResource(diachronResourcePrefix + "RelationalTable");
		resource = ResourceFactory.createResource(diachronResourcePrefix + "Resource");
		schemaClass = ResourceFactory.createResource(diachronResourcePrefix + "SchemaClass");
		schemaProperty = ResourceFactory.createResource(diachronResourcePrefix + "SchemaProperty");
		
		hasInstantiation = ResourceFactory.createProperty(diachronResourcePrefix + "hasInstantiation");
		isInstantiationOf = ResourceFactory.createProperty(diachronResourcePrefix + "isInstantiationOf");
		hasAttribute = ResourceFactory.createProperty(diachronResourcePrefix + "hasAttribute");
		hasAttributeValue = ResourceFactory.createProperty(diachronResourcePrefix + "hasAttributeValue");
		hasDataObject = ResourceFactory.createProperty(diachronResourcePrefix + "hasDataObject");
		hasDimension = ResourceFactory.createProperty(diachronResourcePrefix + "hasDimension");
		hasMeasure = ResourceFactory.createProperty(diachronResourcePrefix + "hasMeasure");
		hasOntologyProperty = ResourceFactory.createProperty(diachronResourcePrefix + "hasOntologyProperty");
		hasProperty = ResourceFactory.createProperty(diachronResourcePrefix + "hasProperty");
		hasRecord = ResourceFactory.createProperty(diachronResourcePrefix + "hasRecord");
		hasRecordAttribute = ResourceFactory.createProperty(diachronResourcePrefix + "hasRecordAttribute");
		hasRecordSet = ResourceFactory.createProperty(diachronResourcePrefix + "hasRecordSet");
		hasChangeSet = ResourceFactory.createProperty(diachronResourcePrefix + "hasChangeSet");
		hasTempRecordSet = ResourceFactory.createProperty(diachronResourcePrefix + "hasTempRecordSet");
		hasRelationalColumn = ResourceFactory.createProperty(diachronResourcePrefix + "hasRelationalColumn");
		hasResourceSet = ResourceFactory.createProperty(diachronResourcePrefix + "hasResourceSet");
		hasSchemaObject = ResourceFactory.createProperty(diachronResourcePrefix + "hasSchemaObject");
		hasSchemaSet = ResourceFactory.createProperty(diachronResourcePrefix + "hasSchemaSet");
		subject = ResourceFactory.createProperty(diachronResourcePrefix + "subject");
		predicate = ResourceFactory.createProperty(diachronResourcePrefix + "predicate");
		object = ResourceFactory.createProperty(diachronResourcePrefix + "object");
		hasDefinition = ResourceFactory.createProperty(diachronResourcePrefix + "hasDefinition"); 
		codelist = ResourceFactory.createProperty(diachronResourcePrefix + "codelist");
		hierarchy = ResourceFactory.createProperty(diachronResourcePrefix + "hierarchy");
		inScheme = ResourceFactory.createProperty(diachronResourcePrefix + "inScheme");
		oldVersion = ResourceFactory.createProperty(changesOntologyNamespace+"old_version");
		newVersion = ResourceFactory.createProperty(changesOntologyNamespace+"new_version");
		paramValue = ResourceFactory.createProperty(changesOntologyNamespace+"param_value");
		
		resultVariable = ResourceFactory.createProperty(sparqlResultsNamespace+"resultVariable");
		solution = ResourceFactory.createProperty(sparqlResultsNamespace+"solution");
		binding = ResourceFactory.createProperty(sparqlResultsNamespace+"binding");
		variable = ResourceFactory.createProperty(sparqlResultsNamespace+"variable");
		value = ResourceFactory.createProperty(sparqlResultsNamespace+"value");
		creationTime = DCTerms.created; 
		
		generatedAtTime = ResourceFactory.createProperty(provNamespace+"generatedAtTime");
		hasFactTable = ResourceFactory.createProperty(provNamespace+"hasFactTable");
		isFullyMaterialized = ResourceFactory.createProperty(diachronResourcePrefix+"isFullyMaterialized");
		addedGraph = ResourceFactory.createProperty(diachronResourcePrefix+"addedGraph"); 
		deletedGraph = ResourceFactory.createProperty(diachronResourcePrefix+"deletedGraph"); 
	}
	
}
