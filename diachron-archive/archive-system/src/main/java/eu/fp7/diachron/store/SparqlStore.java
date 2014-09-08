package eu.fp7.diachron.store;

import java.io.IOException;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * 
 * @author Ruben Navarro Piris
 *
 */
public interface SparqlStore {

  void storeTriples(String targetGraph, Model model) throws IOException;

  void executeUpdate(String update) throws IOException;

  QueryExecution queryExecution(String query) throws IOException;

}
