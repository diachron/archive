package org.athena.imis.diachron.archive.datamapping;

import org.coode.owlapi.rdf.model.RDFTranslator;
import org.semanticweb.owlapi.model.*;

import java.net.URI;
import java.util.Collection;

import static org.semanticweb.owlapi.vocab.OWLRDFVocabulary.RDF_TYPE;

/**
 * @author Simon Jupp
 * @date 10/02/2014
 * Functional Genomics Group EMBL-EBI
 */
public class FilteredRDFVisitor extends RDFTranslator {

    private Collection<URI> filteredPredicates;

    /**
     * @param manager         the manager
     * @param ontology        the ontology
     * @param useStrongTyping
     */
    public FilteredRDFVisitor(OWLOntologyManager manager, OWLOntology ontology, boolean useStrongTyping, Collection<URI> filteredPredicates) {
        super(manager, ontology, useStrongTyping);
        this.filteredPredicates = filteredPredicates;
    }

    @Override
    public void visit(OWLSubClassOfAxiom axiom) {
        if (!axiom.getSuperClass().isAnonymous()) {
            super.visit(axiom);
        }
    }

    @Override
    public void visit(OWLAnnotationAssertionAxiom axiom) {
        if (filteredPredicates.contains(axiom.getProperty().getIRI().toURI())) {
            super.visit(axiom);
        }
    }
}
