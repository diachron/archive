<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/functions"  prefix="fn" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Archive</title>
<link rel="stylesheet"
	href="//code.jquery.com/ui/1.10.4/themes/smoothness/jquery-ui.css">
<link rel="stylesheet" href="css/style.css">
</head>
<body>
	<div id="wrapper">
		<div id="header">
			<div id="logo">
			<img src="images/diachron.png" alt="diachron logo">
			</div>
			<h1>The Archive</h1>
			<div style="clear: both;"></div>
			<h2>Managing the Evolution and Preservation of the Data Web</h2>
		</div>
		<div id="menu">
			<ul>
			<li><a href=".">Home</a></li>
			<li><a href="apidoc">API Documentation</a></li>
			<li><a href="query">Query demo</a></li>
			<li><a href="loader">Data loader</a></li>
			</ul>
		</div>
		<c:import url="${param.inner}"></c:import>
	</div>
	<div id="footer">
		<img src="images/ATHENA_logo.png" id="athena" alt="ATHENA logo">
		<img src="images/eu.png" id="eu" alt="EU logo">
		<h4>FP7 IP Project</h4>
		<h4>April 2013 - March 2016</h4>
		<h4><a href="http://www.diachron-fp7.eu">www.diachron-fp7.eu</a></h4>
		<div style="clear: both;"></div>
			
		</div>
	<script src="//code.jquery.com/jquery-1.10.2.js"></script>
	<script src="//code.jquery.com/ui/1.10.4/jquery-ui.js"></script>
	<script>
	<c:set var="baseURL" value="${fn:replace(pageContext.request.requestURL, pageContext.request.requestURI, pageContext.request.contextPath)}" />
	var host = "${baseURL }/";
	</script>
	<script src="js/presets.js"></script>
	<script src="js/scripts.js"></script>
	
</body>
</html>