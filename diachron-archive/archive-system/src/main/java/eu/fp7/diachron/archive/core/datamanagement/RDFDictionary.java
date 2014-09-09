package eu.fp7.diachron.archive.core.datamanagement;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import eu.fp7.diachron.archive.core.store.Loader;
import eu.fp7.diachron.archive.core.store.SparqlStore;
import eu.fp7.diachron.archive.models.Dataset;
import eu.fp7.diachron.archive.models.DiachronOntology;
import eu.fp7.diachron.archive.models.DiachronicDataset;
import eu.fp7.diachron.archive.models.ModelsFactory;
import eu.fp7.diachron.archive.models.RDFDataset;

/**
 * 
 * Provides a bridge between the dictionary of datasets in the archive and the archive's other
 * functionalities.
 *
 */
public class RDFDictionary implements DictionaryService {

  static final String dictionaryNamedGraph = "XAXA"; // FIXME ????

  /**
   * Fetches the named graph where the dictionary is defined in the archive.
   * 
   * @return A String containing a URI of the dictionary graph.
   */
  public static String getDictionaryNamedGraph() {
    return dictionaryNamedGraph;
  }

  private final SparqlStore sparqlStore;
  private final Loader loader;

  public RDFDictionary(SparqlStore sparqlStore, Loader loader) {
    this.sparqlStore = sparqlStore;
    this.loader = loader;
  }

  /**
   * Creates a diachronic dataset object in the dictionary of datasets.
   * 
   * FIXME the lack of a DataStore interface makes this functionality Virtuoso-only
   * 
   * @param dds The DiachronicDataset to be created.
   * @return A String URI of the created diachronic dataset.
   * @throws IOException 
   * 
   */
  @Override
  public void storeDiachronicDataset(DiachronicDataset dds) throws IOException {
    Model model = ModelFactory.createDefaultModel();

    Resource diachronicDatasetResource =
        model.createResource(dds.getId(), DiachronOntology.diachronicDataset);

    for (String predicate : dds.getMetaPropertiesNames()) {
      String objectString = (String) dds.getMetaProperty(predicate);
      try {
        URL object = new URL(objectString);
        diachronicDatasetResource.addProperty(ResourceFactory.createProperty(predicate),
            model.createResource(object.toString()));
      } catch (MalformedURLException e) {
        diachronicDatasetResource.addProperty(ResourceFactory.createProperty(predicate),
            model.createLiteral(objectString));
      }
    }

    this.loader.loadModel(model, dictionaryNamedGraph);

  }

  /**
   * Create a URI for a new diachronic dataset.
   * @param dds 
   * 
   * @return A string URI.
   */
  @Override
  public String createDiachronicDatasetId() {
    // FIXME this function should not even exist, since dds already has an id
    // TODO proper URI creation
    // sequential or random but at least check for conflict with existing
    double multi = (double) (10 ^ 8);
    int id = (int) (multi * Math.random());

    String URI =
        "http://www.diachron-fp7.eu/resource/diachronicDataset/"
            + Integer.toHexString(id).toLowerCase();
    return URI;
  }

  /**
   * Fetches a list of diachronic datasets, as DiachronicDataset objects, existing in the DIACHRON
   * archive.
   * 
   * @return A list of diachronic datasets existing in the archive.
   * @throws IOException 
   */
  @Override
  public List<DiachronicDataset> getListOfDiachronicDatasets() throws IOException {
    String query = "SELECT DISTINCT ?s FROM <" + RDFDictionary.getDictionaryNamedGraph()
        + "> WHERE {  ?s a <" + DiachronOntology.diachronicDataset + ">} ";
    QueryExecution execution = this.sparqlStore.queryExecution(query);

    try {
      List<DiachronicDataset> diachronicDatasets = new ArrayList<DiachronicDataset>();
      ResultSet rs = execution.execSelect();
      while (rs.hasNext()) {
        QuerySolution qs = rs.next();
        String diachronicDatasetId = qs.get("s").toString();
        DiachronicDataset dds = ModelsFactory.createDiachronicDataset(diachronicDatasetId);
        diachronicDatasets.add(dds);
      }
      return diachronicDatasets;
    } finally {
      execution.close();
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException 
   */
  @Override
  public List<Dataset> getListOfDatasets(DiachronicDataset diachronicDataset) throws IOException {
    String query = "SELECT DISTINCT ?o FROM <" + RDFDictionary.getDictionaryNamedGraph()
        + "> WHERE {  <" + diachronicDataset.getId() + "> <" + DiachronOntology.hasInstantiation
        + "> ?o . " + "?o <" + DiachronOntology.generatedAtTime + "> ?time} ORDER BY DESC(?time) ";
    QueryExecution execution = this.sparqlStore.queryExecution(query);
    try {
      List<Dataset> datasets = new ArrayList<Dataset>();
      ResultSet rs = execution.execSelect();
      while (rs.hasNext()) {
        QuerySolution qs = rs.next();
        String datasetId = qs.get("o").toString();
        // Dataset ds = ModelsFactory.createDataset(diachronicDataset);
        Dataset ds = new RDFDataset(datasetId);
        datasets.add(ds);
      }
      return datasets;
    } finally {
      execution.close();
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException 
   */
  public Hashtable<String, Object> getDiachronicDatasetMetadata(String diachronicDatasetId) throws IOException {
    String query = "SELECT ?p ?o FROM <" + RDFDictionary.getDictionaryNamedGraph() + "> WHERE {  <"
        + diachronicDatasetId + "> ?p ?o " + " FILTER (?p!= <" + DiachronOntology.hasInstantiation
        + ">) } ";
    QueryExecution execution = this.sparqlStore.queryExecution(query);
    try {
      Hashtable<String, Object> metaProperties = new Hashtable<String, Object>();

      ResultSet rs = execution.execSelect();
      while (rs.hasNext()) {
        QuerySolution qs = rs.next();
        String value = qs.get("o").toString();
        String name = qs.get("p").toString();
        metaProperties.put(name, value);
      }
      return metaProperties;
    } finally {
      execution.close();
    }
  }

  /**
   * {@inheritDoc}
   * @throws IOException 
   */
  @Override
  public DiachronicDataset getDiachronicDataset(String id) throws IOException {
    DiachronicDataset dds = ModelsFactory.createDiachronicDataset(id);
    dds.setMetaProperties(getDiachronicDatasetMetadata(id));
    for (Dataset dataset : getListOfDatasets(dds))
      dds.addDatasetInstatiation(dataset);
    return dds;
  }

}