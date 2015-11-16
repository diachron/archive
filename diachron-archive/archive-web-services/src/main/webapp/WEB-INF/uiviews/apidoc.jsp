<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/functions"  prefix="fn" %>
		<div id="content">
			<h2>Archive API documentation</h2>
			<p>Properly structured query requests must contain the following parameters:</p>
			<ul>
				<li>'query'    : The URL-encoded query string expressed in SPARQL</li>
				<li>'queryType : The query type (e.g. 'CONSTRUCT' or 'SELECT')</li>  
			</ul>
                        <c:set var="url">${pageContext.request.requestURL}</c:set>
			<c:set var="baseURL"
				value="/" />
			
			<p>Example request: <br/>
			<a target="_blank" href="${baseURL}/archive?query=CONSTRUCT%20{?dataset%20?p%20?o}%20FROM%20&lt;efo-datasets-v3&gt;%20WHERE%20{?dataset%20a%20diachron:Dataset%20;%20?p%20?o}&queryType=CONSTRUCT">
			${baseURL}/archive?query=CONSTRUCT%20{?dataset%20?p%20?o}%20FROM%20&lt;efo-datasets-v3&gt;%20WHERE%20{?dataset%20a%20diachron:Dataset%20;%20?p%20?o}&queryType=CONSTRUCT</a></p>
			<p>The response is a JSON object that contains a "message" parameter and a "data" parameter.</p> 
			<p>The returned results are contained in the parameter "data" of the returned JSON object, serialized in RDF/JSON according to the <a href="http://www.w3.org/TR/rdf-json/">RDF/JSON specification</a>.</p>
			<p>For SELECT queries, the returned format follows the <a href="http://www.w3.org/TR/sparql11-results-json/">SPARQL Results specification.</a></p>
			<p>CONSTRUCT queries return fully-compliant RDF graphs, the returned format of which is RDF/JSON.</p> 
		</div>
		
