<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring"%>

		<div id="content">
			<h2>Query playground</h2>
			<p>Query demo page used for testing the archive service</p>
			<div id="queryArea">
			<textarea  id="queryText" placeholder="place your SPARQL here"></textarea>
			<strong>Query type:</strong>
			Select <input type="radio" name="queryType" value="SELECT" checked="checked"/>
			Construct <input type="radio" name="queryType" value="CONSTRUCT"/>
			</div>
			<div id="results">
			<textarea placeholder="results" readonly="readonly"></textarea>
			</div>
			<div style="clear: both;"></div>
			<br /> <input id="queryBtn" type="button" value="Query">
			<h3>Preset queries</h3>
			<div id="presetsArea"></div>

			<div id="loading-div">
				<div id="loading-div-background"></div>
				<div id="loading-div-inner" class="ui-corner-all">
					<img src="images/loading3.gif" alt="Loading.." />
					<h2>Please wait....</h2>
				</div>
			</div>
		</div>
		
		
	