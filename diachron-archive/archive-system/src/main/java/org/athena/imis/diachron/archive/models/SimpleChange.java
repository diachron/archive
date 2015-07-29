package org.athena.imis.diachron.archive.models;

import java.util.HashMap;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class SimpleChange {

	private Node subject;
	private Node predicate;
	private Node object;
	private String add_or_del;
	private String changeType;
	public static HashMap<String, Property> propertyMap;
	public static HashMap<String, Resource> objectMap ;
	
	static {
		initializePropertyMap();
	}
	
	@Override
	public String toString(){
		return "Type: " + changeType + ", {s,p,o}: " + subject.toString() + ", "+predicate.toString()+", " + object.toString();
	}
	public SimpleChange(String changeType, Node o1, Node o2, Node o3) throws Exception{
		
		this.changeType = changeType; 
		if(changeType.contains("Add_")) 
			add_or_del = "add";
		
		else if(changeType.contains("Delete_")) 
			add_or_del = "delete";
		
		else 
			throw new Exception("Unrecognized simple change");
		
		if(o3 == null) //no property hence only one or two params
		{
			if(propertyMap.containsKey(changeType))
			{
				predicate = NodeFactory.createURI(propertyMap.get(changeType).toString());
				if(o1==null) 
					throw new Exception();			
				else
				{
					subject = o1;
					if(o2 == null){
						object = NodeFactory.createURI(objectMap.get(changeType).getURI().toString());
					}
					else
						object = o2;
				}
			}
				
			else
				throw new Exception();				
		}				
		
		else if(changeType.equals("http://www.diachron-fp7.eu/changes/Add_Property_Instance")
				|| changeType.equals("http://www.diachron-fp7.eu/changes/Delete_Property_Instance"))
		{
			if(o1==null || o2==null) 
				throw new Exception();			
			else
			{
				subject = o1;
				predicate = o2;
				object = o3;
			}
			
		}
		
		
	}
	
	public String getAddOrDel(){
		return this.add_or_del;
	}
	
	public static void initializePropertyMap(){
		
		propertyMap = new HashMap<String, Property>();
		objectMap = new HashMap<String, Resource>();
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Label", RDFS.label);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Comment", RDFS.comment);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Domain", RDFS.domain);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Range", RDFS.range);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Superclass", RDFS.subClassOf);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Superproperty", RDFS.subPropertyOf);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Class", RDF.type);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Property", RDF.type);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Add_Type_To_Individual", RDF.type);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Label", RDFS.label);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Comment", RDFS.comment);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Domain", RDFS.domain);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Range", RDFS.range);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Superclass", RDFS.subClassOf);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Superproperty", RDFS.subPropertyOf);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Type_Class", RDF.type);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Type_Property", RDF.type);
		propertyMap.put("http://www.diachron-fp7.eu/changes/Delete_Type_To_Individual", RDF.type);
		objectMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Class", OWL.Class);
		objectMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Property", ResourceFactory.createResource(OWL.NAMESPACE+"Property"));
		//objectMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Class", OWL.Class);
		objectMap.put("http://www.diachron-fp7.eu/changes/Delete_Type_Class", OWL.Class);
		objectMap.put("http://www.diachron-fp7.eu/changes/Delete_Type_Property", ResourceFactory.createResource(OWL.NAMESPACE+"Property"));
		//objectMap.put("http://www.diachron-fp7.eu/changes/Add_Type_Class", OWL.Class);
		
	}
	
	public Triple getTriple(){
		return new Triple(subject, predicate, object);		
	}
	
	public Resource getSubject(){
		return ResourceFactory.createResource(subject.getURI());		
	}
	
	public Property getPredicate(){
		return ResourceFactory.createProperty(predicate.getURI());	
	}
	
	public Node getObject(){		
		//if(object.isURI())	return ResourceFactory.createResource(object.getURI()).asNode();
		return object;
	}
	
	public void setSubject(Node subject){
		this.subject = subject;
	}
	
	public void setPredicate(Node predicate){
		this.predicate = predicate;
	}
	
	public void setObject(Node object){
		this.object = object;
	}

	
	
}
