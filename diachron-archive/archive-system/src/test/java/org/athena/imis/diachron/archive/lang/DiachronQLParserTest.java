package org.athena.imis.diachron.archive.lang;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.lang.SPARQLParser;

public class DiachronQLParserTest {
	public static void main(String[] args) {
		simpleTest();
	}

	private static void simpleTest() {
		Query query = new DiachronQuery();
		SPARQLParser parser = new DiachronParserSPARQL11();
		String queryString ;
		//String queryString = "DIACHRON SELECT * WHERE {?s ?p ?o}";
		queryString = "DIACHRON NO_SCHEMA "
				+ " SELECT  ?s ?p ?o FROM DATASET <http://www.diachron-fp7.eu/resource/diachronicDataset/efo/475B9ABBF2FA36351EE30C79F440719B> "
				+ " WHERE { RECORD { ?s ?p ?o} } LIMIT 1000";
		
		//queryString = "DIACHRON NO_SCHEMA SELECT ?s FROM "
		queryString = " DIACHRON NO_SCHEMA "
		+ " SELECT ?dataset ?version ?rec ?s ?p ?o "
		+ " WHERE { "
		+ " DATASET ?dataset AT_VERSION ?version { "
		+ "  RECORD ?rec { ?s ?pp ?oo RECATT  {?p ?o} } } } LIMIT 1000 ";

		query = parser.parse(query, queryString);

		System.out.println(((DiachronQuery) query).isDiachronQuery());

		System.out.println(query.serialize());
	}

}
