var presets = [ {
			query : "SELECT ?dataset ?p ?o FROM <efo-datasets-v3> WHERE {?dataset a diachron:DiachronicDataset ; ?p ?o}",
			queryType: "SELECT",
			title : "preset 1",
			description : "Retrieve all diachronic datasets in the Dictionary of datasets graph and their triples. "
		},
		{
			query : "SELECT ?dataset ?p ?o FROM <efo-datasets-v3> WHERE {?dataset a diachron:Dataset ; ?p ?o}",
			queryType: "SELECT",
			title : "preset 2",
			description : "Retrieve all dataset versions in the Dictionary of datasets graph. "
		},
		{
			query : "select distinct ?changeset ?change ?changeType ?consumedBy ?value ?parameter ?v1 ?v2  where { graph <efo-datasets-v3> {?changeset co:old_version ?v1; co:new_version ?v2 . } . graph ?changeset {?change a ?ct ; ?c_par1 [co:param_value <http://purl.obolibrary.org/obo/BTO_0000664>]. ?ct co:name ?changeType. optional{?change ?c_par2 ?par2 . ?par2 co:param_value ?value filter (?value!=<http://purl.obolibrary.org/obo/BTO_0000664>). graph <efo-changes-full> {?par2 a [co:name ?parameter] } } optional {?consumedBy co:consumes ?change. }}} order by ?v1 ?v2",
			queryType : "SELECT",
			title : "preset_3",
			description : "Retrieve changes from all dataset versions containing http://purl.obolibrary.org/obo/BTO_0000664"
		}
		];

		