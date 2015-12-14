package org.athena.imis.diachron.archive.datamapping;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.athena.imis.diachron.archive.models.Dataset;
import org.athena.imis.diachron.archive.models.DiachronOntology;
import org.athena.imis.diachron.archive.models.DiachronURIFactory;
import org.athena.imis.diachron.archive.models.DiachronicDataset;
import org.athena.imis.diachron.archive.models.ModelsFactory;
import org.athena.imis.diachron.archive.models.RDFDataset;
import org.athena.imis.diachron.archive.models.RDFDiachronicDataset;
import org.athena.imis.diachron.archive.models.RDFRecordSet;
import org.athena.imis.diachron.archive.models.Record;
import org.athena.imis.diachron.archive.models.RecordAttribute;
import org.athena.imis.diachron.archive.models.RecordSet;
import org.coode.owlapi.rdf.model.RDFGraph;
import org.coode.owlapi.rdf.model.RDFLiteralNode;
import org.coode.owlapi.rdf.model.RDFResourceNode;
import org.coode.owlapi.rdf.model.RDFTriple;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.elk.owlapi.ElkReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;
import org.semanticweb.owlapi.util.InferredSubClassAxiomGenerator;
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.vocabulary.RDF;


/**
 * This class implements a converter from ontological source models to the DIACHRON model. Properties can be filtered.
 * @author Marios Meimaris
 *
 */
public class OntologyConverter implements DataConverter {
	private OWLOntologyManager manager;
	private Dataset dataset;
	private DiachronicDataset diachronicDataset;

	private static final Logger logger = LoggerFactory
			.getLogger(OntologyConverter.class);

	/**
	 * Converts a source model from the input stream, using a dataset label,  a collection of property filters and a specific reasoner.
	 * @param input The input model to be converted.
	 * @param output The output stream where the converted model will be serialized.
	 * @param datasetName The label of the dataset to be used in the created URI. 
	 * @param filter A collection of property URIs to be filtered out from the conversion process.
	 * @param reasoner The label of the reasoner to be used. Supported reasoners are Hermit, Elk, and none, which defaults to 
	 * the OWLAPI StructuralReasoner.
	 */
	public void convert(InputStream input, OutputStream output, String datasetName, Collection<URI> filter, String reasoner) {

		String version = (new Date()).getTime() + "";
		convert(input, datasetName, version, filter, reasoner);
		Model model = getJenaModelFromDataset();
		model.write(output);

	}
	
	/**
	 * Converts a source model from the input stream, using a dataset label and a collection of property filters.
	 * @param input The input model to be converted.
	 * @param output The output stream where the converted model will be serialized.
	 * @param datasetName The label of the dataset to be used in the created URI. 
	 * @param filter A collection of property URIs to be filtered out from the conversion process. 
	 */
	public void convert(InputStream input, OutputStream output, String datasetName, Collection<URI> filter) {

		String version = (new Date()).getTime() + "";
		convert(input, datasetName, version, filter, null);
		Model model = getJenaModelFromDataset();
		model.write(output);

	}
	
	/**
	 * Converts a source model from the input stream, using a dataset label.
	 * @param input The input model to be converted.
	 * @param output The output stream where the converted model will be serialized.
	 * @param datasetName The label of the dataset to be used in the created URI. 	 
	 */
	public void convert(InputStream input, OutputStream output,  String datasetName) {

		String version = (new Date()).getTime() + "";
		convert(input, datasetName, version, null, null);		
		Model model = getJenaModelFromDataset();
		model.write(output);

	}
	
	/**
	 * Converts a source model from the input stream, using a dataset label and a specific reasoner.
	 * @param input The input model to be converted.
	 * @param output The output stream where the converted model will be serialized.
	 * @param reasoner The label of the reasoner to be used. Supported reasoners are Hermit, Elk, and none, which defaults to 
	 * the OWLAPI StructuralReasoner.
	 * @param datasetName The label of the dataset to be used in the created URI. 	 
	 */
	public void convert(InputStream input, OutputStream output,  String datasetName, String reasoner) {

		String version = (new Date()).getTime() + "";
		convert(input, datasetName, version, null, reasoner);		
		Model model = getJenaModelFromDataset();
		model.write(output);

	}

	/**
	 * Returns a jena model with the converted dataset.
	 * @return A jena Model object containing the converted dataset along with its metadata.
	 */
	public Model getJenaModelFromDataset() {
		Model model = ModelFactory.createDefaultModel();

		// create the diachron dataset
		// TODO

		Statement s1 = new StatementImpl(new ResourceImpl(diachronicDataset
				.getId().toString()), RDF.type, new ResourceImpl(
				DiachronOntology.diachronicDataset.getURI().toString()));

		// create the dataset
		Statement s2 = new StatementImpl(new ResourceImpl(dataset.getId()
				.toString()), RDF.type, new ResourceImpl(
				DiachronOntology.dataset.getURI().toString()));

		model.add(s1);
		model.add(s2);
		model.add(s1.getSubject(), new PropertyImpl(
				DiachronOntology.hasInstantiation.getURI().toString()), s2
				.getSubject());
		
		// create the record set
		Statement s3 = new StatementImpl(new ResourceImpl(dataset
				.getRecordSet().getId()), RDF.type, new ResourceImpl(
				DiachronOntology.recordSet.getURI().toString()));

		model.add(s3);
		model.add(s2.getSubject(), new PropertyImpl(
				DiachronOntology.hasRecordSet.getURI().toString()), s3
				.getSubject());

		for (Record record : dataset.getRecordSet().getRecords()) {

			Statement s4 = new StatementImpl(new ResourceImpl(record.getId()
					.toString()), RDF.type, new ResourceImpl(
					DiachronOntology.record.getURI().toString()));
			
			model.add(s4);
			
			model.add(s3.getSubject(), new PropertyImpl(
					DiachronOntology.hasRecord.getURI().toString()), s4
					.getSubject());			
			
			model.add(s4.getSubject(), new PropertyImpl(
					DiachronOntology.subject.getURI().toString()),
					new ResourceImpl(record.getSubject().toString()));

			for (RecordAttribute attribute : record.getRecordAttributes()) {

				Statement s5 = new StatementImpl(new ResourceImpl(
						attribute.getId()), RDF.type, new ResourceImpl(
						DiachronOntology.recordAttribute.getURI().toString()));
				model.add(s5);
				model.add(s4.getSubject(),
						new PropertyImpl(DiachronOntology.hasRecordAttribute
								.getURI().toString()), s5.getSubject());

				model.add(s5.getSubject(), new PropertyImpl(
						DiachronOntology.predicate.getURI().toString()),
						new ResourceImpl(attribute.getProperty()));

				if (attribute.getPropertyValueIsLiteral())
					model.add(s5.getSubject(), new PropertyImpl(
							DiachronOntology.object.getURI().toString()),
							attribute.getPropertyValue());
				else
					model.add(s5.getSubject(), new PropertyImpl(
							DiachronOntology.object.getURI().toString()),
							new ResourceImpl(attribute.getPropertyValue()));

			}
		}
		return model;
	}

	/**
	 * Covnerts the dataset found in the input to java objects from the DIACHRON models package.
	 * @param input The input stream containing the source dataset.
	 * @param datasetName The label of the dataset.
	 * @param version A version number under a custom numbering scheme defined by the user.
	 * @param predicateFilters A collection of property URIs to be filtered out from the conversion process.
	 */
	private void convert(InputStream input, String datasetName, String version,
			Collection<URI> predicateFilters, String reasoner) {

		try {
			DiachronURIFactory uriFactory = new DiachronURIFactory(datasetName, version);
			diachronicDataset = new RDFDiachronicDataset();
			diachronicDataset.setId(uriFactory.generateDiachronicDatasetUri().toString());
			this.manager = OWLManager.createOWLOntologyManager();

			
			// load ontology into OWLAPI
			OWLOntology ontology = manager.loadOntologyFromOntologyDocument(input);
			
     		// create a reasoner factory and classify ontology
			OWLReasoner owlReasoner = null;

			if (reasoner != null && reasoner.equals("elk")) {
				
			    ElkReasonerFactory elkReasonerFactory = new ElkReasonerFactory();
			    
			    owlReasoner = elkReasonerFactory.createReasoner(ontology);
			    
			} else if (reasoner != null && reasoner.equals("hermit")) {
				
			    Reasoner.ReasonerFactory owlReasonerFactory = new Reasoner.ReasonerFactory();
			    
			    owlReasoner = owlReasonerFactory.createReasoner(ontology);
			    
			} else {
				
			    OWLReasonerFactory structuralReasonerFactory = new StructuralReasonerFactory();
			    
			    owlReasoner = structuralReasonerFactory.createReasoner(ontology);
			    
			}
			   
			/*OWLOntology ontology = manager.loadOntologyFromOntologyDocument(input);

			// create a reasoner factory and classify ontology
			Reasoner.ReasonerFactory owlReasonerFactory = new Reasoner.ReasonerFactory();
			OWLReasoner owlReasoner = owlReasonerFactory.createReasoner(ontology);*/

			OWLOntology reasonedOntology = owlReasoner.getRootOntology();
			FilteredRDFVisitor visitor = new FilteredRDFVisitor(manager, reasonedOntology, true, predicateFilters);

			InferredSubClassAxiomGenerator inferredAxioms = new InferredSubClassAxiomGenerator();
			Set<OWLSubClassOfAxiom> subClasses = inferredAxioms.createAxioms(
					manager, owlReasoner);

			for (OWLSubClassOfAxiom subClass : subClasses)
				visitor.visit(subClass);

			for (OWLClass entity : reasonedOntology.getClassesInSignature()) {

				for (OWLDeclarationAxiom declarationAxiom : reasonedOntology
						.getDeclarationAxioms(entity))
					visitor.visit(declarationAxiom);
			

				for (OWLAnnotationAssertionAxiom annotation : reasonedOntology
						.getAnnotationAssertionAxioms(entity.getIRI()))
					visitor.visit(annotation);

			}

			
			this.dataset = new RDFDataset();
			

			dataset.setId(uriFactory.generateDatasetUri().toString());
			RDFGraph graph = visitor.getGraph();

			RecordSet rs = new RDFRecordSet();
			rs.setId(uriFactory.generateDiachronRecordSetURI().toString());
			dataset.setRecordSet(rs);

			for (OWLClass entity : reasonedOntology.getClassesInSignature()) {
		
				Record rec = ModelsFactory.createRecord();
				rec.setId(uriFactory.generateRecordUri(entity.getIRI().toURI()).toString());
				rec.setSubject(entity.getIRI().toURI().toString());
				rs.addRecord(rec);

				for (RDFTriple triple : graph.getTriplesForSubject(
						new RDFResourceNode(entity.getIRI()), true)) {
					
					RecordAttribute recAttr = ModelsFactory
							.createRecordAttribute();

					if (triple.getObject().isLiteral()) {
						recAttr.setId(uriFactory.generateRecordAttributeUri(
								entity.getIRI().toURI(),
								triple.getProperty().getIRI().toURI(),
								((RDFLiteralNode) triple.getObject())
										.getLiteral()).toString());
						recAttr.setProperty(triple.getProperty().getIRI()
								.toString());
						recAttr.setPropertyValue(((RDFLiteralNode) triple
								.getObject()).getLiteral());
						recAttr.setPropertyValueIsLiteral();
					} else {
						recAttr.setId(uriFactory.generateRecordAttributeUri(
								entity.getIRI().toURI(),
								triple.getProperty().getIRI().toURI(),
								triple.getObject().getIRI().toURI().toString())
								.toString());
						recAttr.setProperty(triple.getProperty().getIRI()
								.toString());
						recAttr.setPropertyValue(triple.getObject().getIRI()
								.toURI().toString());

					}
					rec.addRecordAttribute(recAttr);					
				}
			}			
		} catch (OWLOntologyCreationException e) {
			logger.error(e.getMessage(), e);
		}
	}
	
	
}
