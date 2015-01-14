package org.athena.imis.diachron.archive.models;

import java.util.Iterator;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * This class implements a serializer for RDF data.
 *
 */
public class RDFSerializer implements Serializer {

	public String serialize(List<? extends DiachronEntity> list)
			throws Exception {
		/*
		 * Method method = this.getClass().getEnclosingMethod();
		 * ParameterizedType listType =
		 * (ParameterizedType)method.getGenericParameterTypes()[0]; Type
		 * classType = listType.getActualTypeArguments()[0];
		 */
		// Model resultModel = ModelFactory.createDefaultModel();
		// Resource rset = resultModel.createResource(AnonId.create("rset"));
		// if (classType.getClass().equals(DiachronicDataset.class)) {
		if (null == list)
			return null;
		if (list.size() == 0)
			return "";
		
		JSONObject json = new JSONObject();
		JSONObject vars = new JSONObject();
		JSONArray varsArray = new JSONArray();
		JSONObject results = new JSONObject();
		JSONArray bindingsArray = new JSONArray();

		if (list.get(0) instanceof DiachronicDataset) {

			String variableName = "diachronicDataset";
			varsArray.put(variableName);
			varsArray.put("label");
			varsArray.put("creator");
			varsArray.put("creationTime");
			varsArray.put("datasetName");
			
			vars.put("vars", varsArray);
			json.put("head", vars);

			Iterator<DiachronicDataset> listIt = (Iterator<DiachronicDataset>) list
					.iterator();
			while (listIt.hasNext()) {
				DiachronicDataset dd = listIt.next();
				JSONObject result = new JSONObject();
				JSONObject row = new JSONObject();
				row.put("type", "uri");
				row.put("value", dd.getId());
				result.put(variableName, row);
				
				row = createMetadataRow(dd, RDFS.label.toString());
				if (row != null)
					result.put("label", row); 
				
				row = createMetadataRow(dd, DCTerms.creator.toString());
				if (row != null)
					result.put("creator", row); 
				
				row = createMetadataRow(dd, DCTerms.created.toString());
				if (row != null)
					result.put("creationTime", row); 
				else  {
					row = createMetadataRow(dd, DiachronOntology.generatedAtTime.toString());
					if (row != null)
						result.put("creationTime", row); 
				}
				
				row = createMetadataRow(dd, DCTerms.title.toString());
				if (row != null)
					result.put("datasetName", row); 
				
				bindingsArray.put(result);
				
				/*
				 * Resource binding =
				 * resultModel.createResource(AnonId.create("r"
				 * +count+"c"+count));
				 * binding.addProperty(DiachronOntology.variable, variableName)
				 * .addProperty(DiachronOntology.value,
				 * resultModel.createResource(dd.getId())); Resource solution =
				 * resultModel.createResource(AnonId.create("r"+count));
				 * solution.addProperty(DiachronOntology.binding, binding);
				 * rset.addProperty(DiachronOntology.solution, solution);
				 * count++;
				 */
			}
			results.put("bindings", bindingsArray);
			json.put("results", results);

			// rset.addProperty(DiachronOntology.resultVariable, variableName);

			// } else if (classType.getClass().equals(Dataset.class)) {
		} else if (list.get(0) instanceof Dataset) {
			Iterator<Dataset> listIt = (Iterator<Dataset>) list.iterator();
			String variableName = "dataset";
			while (listIt.hasNext()) {
				Dataset dd = listIt.next();
				JSONObject result = new JSONObject();
				JSONObject row = new JSONObject();
				row.put("type", "uri");
				row.put("value", dd.getId());
				result.put(variableName, row);
				bindingsArray.put(result);
			}
			results.put("bindings", bindingsArray);
			json.put("results", results);
			/*
			 * Resource binding =
			 * resultModel.createResource(AnonId.create("r"+count+"c"+count));
			 * binding.addProperty(DiachronOntology.variable, variableName)
			 * .addProperty(DiachronOntology.value,
			 * resultModel.createResource(dd.getId())); Resource solution =
			 * resultModel.createResource(AnonId.create("r"+count));
			 * solution.addProperty(DiachronOntology.binding, binding);
			 * rset.addProperty(DiachronOntology.solution, solution); count++; }
			 * rset.addProperty(DiachronOntology.resultVariable, variableName);
			 */

		} else {
			throw new Exception("Unsupported type for serialization");
		}
		// ArchiveResultSet ars = new ArchiveResultSet();
		// ars.setJenaResultSet(letsee);
		return json.toString();

	}
	
	private JSONObject createMetadataRow(DiachronicDataset dd, String propertyName) {
		if (dd.getMetaProperty(propertyName) != null) {
			JSONObject row = new JSONObject();
			row.put("type", "literal");
			row.put("value", dd.getMetaProperty(propertyName).toString());
			return row;
		} else {
			return null;
		}
		
	}

}
