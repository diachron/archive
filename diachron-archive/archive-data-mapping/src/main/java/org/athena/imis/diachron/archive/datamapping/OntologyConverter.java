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
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLAnnotationAssertionAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDeclarationAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.util.InferredSubClassAxiomGenerator;
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import com.hp.hpl.jena.rdf.model.impl.StatementImpl;
import com.hp.hpl.jena.vocabulary.RDF;


public class OntologyConverter implements DataConverter {
	private OWLOntologyManager manager;
    private Dataset dataset;
    private DiachronicDataset diachronicDataset;
    
    @Override
    public void convert(InputStream input, OutputStream output) {

        Collection<URI> filter = new HashSet<URI>();
        filter.add(OWLRDFVocabulary.RDFS_LABEL.getIRI().toURI());
        filter.add(URI.create("http://www.ebi.ac.uk/efo/reason_for_obsolescence"));
        filter.add(URI.create("http://www.ebi.ac.uk/efo/definition"));
        filter.add(URI.create("http://www.ebi.ac.uk/efo/alternative_term"));


        String version  = (new Date()).getTime()+""; 
        //OntologyConverter converter = new OntologyConverter();
        //converter.
        convert(
                input,
                "efo",
                version,
                filter );
        //System.out.println(dataset.getId());
        Model model = getJenaModelFromDataset();
        model.write(output);
        
    
    }
    
    
    
    public Model getJenaModelFromDataset() {
    	Model model = ModelFactory.createDefaultModel();
    	
        // create the diachron dataset
    	//TODO
    	
        Statement s1 = new StatementImpl(new ResourceImpl(diachronicDataset.getId().toString()),
                RDF.type,
                new ResourceImpl(DiachronOntology.diachronicDataset.getURI().toString()));


        // create the dataset
        Statement s2 = new StatementImpl(new ResourceImpl(dataset.getId().toString()),
                RDF.type,
                new ResourceImpl(DiachronOntology.dataset.getURI().toString()));

        model.add(s1);
        model.add(s2);
        model.add(s1.getSubject(), new PropertyImpl(DiachronOntology.hasInstantiation.getURI().toString()), s2.getSubject());
        
        //TODO these should go to diachronic dataset
        //model.add(s2.getSubject(), DC.creator, "EBI");
        //model.add(s2.getSubject(), DC.title, "EFO Ontology");

        // create the record set
        Statement s3 = new StatementImpl(
                new ResourceImpl(dataset.getRecordSet().getId()),
                RDF.type,
                new ResourceImpl(DiachronOntology.recordSet.getURI().toString()));

        model.add(s3);
        model.add(s2.getSubject(), new PropertyImpl(DiachronOntology.hasRecordSet.getURI().toString()), s3.getSubject());

        for (Record record : dataset.getRecordSet().getRecords()) {

            Statement s4 = new StatementImpl(
                    new ResourceImpl(record.getId().toString()),
                    RDF.type,
                    new ResourceImpl(DiachronOntology.record.getURI().toString()));
            model.add(s4);
            model.add(s3.getSubject(), new PropertyImpl(DiachronOntology.hasRecord.getURI().toString()), s4.getSubject());
            //System.out.println(record.getId());
            model.add(s4.getSubject(), new PropertyImpl(DiachronOntology.subject.getURI().toString()), new ResourceImpl(record.getSubject().toString()));

            for (RecordAttribute attribute : record.getRecordAttributes()) {

                Statement s5 = new StatementImpl(
                        new ResourceImpl(attribute.getId()),
                        RDF.type,
                        new ResourceImpl(DiachronOntology.recordAttribute.getURI().toString()));
                model.add(s5);
                model.add(s4.getSubject(), new PropertyImpl(DiachronOntology.hasRecordAttribute.getURI().toString()), s5.getSubject());

                model.add(s5.getSubject(), new PropertyImpl(DiachronOntology.predicate.getURI().toString()), new ResourceImpl(attribute.getProperty()));

                if(attribute.getPropertyValueIsLiteral())
                	model.add(s5.getSubject(), new PropertyImpl(DiachronOntology.object.getURI().toString()), attribute.getPropertyValue());                
                else 
                	model.add(s5.getSubject(), new PropertyImpl(DiachronOntology.object.getURI().toString()), new ResourceImpl(attribute.getPropertyValue()));

                /*
                if (attribute instanceof ResourceAttribute) {
                    model.add(s5.getSubject(), new PropertyImpl(DiachronOntology.object.getURI().toString()), new ResourceImpl(((ResourceAttribute) attribute).getObject().toString()));
                }
                else if (attribute instanceof LiteralAttribute) {
                    model.add(s5.getSubject(), new PropertyImpl(DiachronOntology.object.getURI().toString()), ((LiteralAttribute)attribute).getValue());
                }
                else {
                    throw new UnsupportedOperationException();
                }
                */
            }
        }
        return model;
    }

    private void convert(InputStream input, String datasetName, String version, Collection<URI> predicateFilters) {

    try {
    	DiachronURIFactory uriFactory = new DiachronURIFactory(datasetName, version);
    	diachronicDataset = new RDFDiachronicDataset();
    	diachronicDataset.setId(uriFactory.generateDiachronicDatasetUri().toString());
        this.manager = OWLManager.createOWLOntologyManager();
        // load ontology into OWLAPI
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(input);

        // create a reasoner factory and classify ontology
        Reasoner.ReasonerFactory owlReasonerFactory = new Reasoner.ReasonerFactory();
        OWLReasoner owlReasoner = owlReasonerFactory.createReasoner(ontology);

        OWLOntology reasonedOntology = owlReasoner.getRootOntology();
        FilteredRDFVisitor visitor = new FilteredRDFVisitor(manager, reasonedOntology, true, predicateFilters);

        
        InferredSubClassAxiomGenerator inferredAxioms = new InferredSubClassAxiomGenerator();
		Set<OWLSubClassOfAxiom> subClasses = inferredAxioms.createAxioms(manager, owlReasoner);
		
		for (OWLSubClassOfAxiom subClass : subClasses)
			visitor.visit(subClass);
		
        for (OWLClass entity : reasonedOntology.getClassesInSignature()) {

            for (OWLDeclarationAxiom declarationAxiom: reasonedOntology.getDeclarationAxioms(entity))
                visitor.visit(declarationAxiom);

            /*for (OWLSubClassOfAxiom subClasses : reasonedOntology.getSubClassAxiomsForSubClass(entity))
                visitor.visit(subClasses);*/

            for (OWLAnnotationAssertionAxiom annotation : reasonedOntology.getAnnotationAssertionAxioms(entity.getIRI()))
                visitor.visit(annotation);

        }

        // create a new diachronic dataset based on this ontology instance
        URI ontologyUri = ontology.getOntologyID().getOntologyIRI().toURI();
        String ontologyUriAsString = ontologyUri.toString();
        if (ontologyUriAsString.endsWith("/")) {
            ontologyUriAsString = ontologyUriAsString.substring(0, ontologyUriAsString.lastIndexOf("/"));
        }
        this.dataset = new RDFDataset(); 
        		//new DiachronDataset(URI.create(ontologyUriAsString), name, version);

        dataset.setId(uriFactory.generateDatasetUri().toString());
        RDFGraph graph = visitor.getGraph();
        
        RecordSet rs = new RDFRecordSet();
        rs.setId(uriFactory.generateDiachronRecordSetURI().toString());
        dataset.setRecordSet(rs);
        
        for (OWLClass entity : reasonedOntology.getClassesInSignature()) {

            //System.out.println("Triples for " + entity.getIRI().toURI().toString());
        
            
            Record rec = ModelsFactory.createRecord();
            rec.setId(uriFactory.generateRecordUri(entity.getIRI().toURI()).toString());
            rec.setSubject(entity.getIRI().toURI().toString());
            rs.addRecord(rec);
            //System.out.println(entity.getIRI().toURI());
            
            for (RDFTriple triple : graph.getTriplesForSubject(new RDFResourceNode(entity.getIRI()), true)) {

                //System.out.println("\t" + triple.toString());

                RecordAttribute recAttr = ModelsFactory.createRecordAttribute();
                
                if (triple.getObject().isLiteral()) {
                	recAttr.setId(uriFactory.generateRecordAttributeUri(entity.getIRI().toURI(), 
                    		triple.getProperty().getIRI().toURI(),
                            ((RDFLiteralNode)triple.getObject()).getLiteral()).toString());
                	recAttr.setProperty(triple.getProperty().getIRI().toString());
                	recAttr.setPropertyValue(((RDFLiteralNode)triple.getObject()).getLiteral());
                	recAttr.setPropertyValueIsLiteral();
                }
                else {
                	recAttr.setId(uriFactory.generateRecordAttributeUri(entity.getIRI().toURI(), 
                    		triple.getProperty().getIRI().toURI(),
                    		triple.getObject().getIRI().toURI().toString()).toString());
                	recAttr.setProperty(triple.getProperty().getIRI().toString());
                	recAttr.setPropertyValue(triple.getObject().getIRI().toURI().toString());
                	
                }
                rec.addRecordAttribute(recAttr);
                //System.out.println(recAttr.getPropertyValue());
            }
        }
        //System.out.println(dataset.getRecordSet().getId());
    } catch (OWLOntologyCreationException e) {
        e.printStackTrace();
    }
}



}
