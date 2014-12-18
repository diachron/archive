<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@ taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/functions"  prefix="fn" %>
		<div id="content">
			<h2>Dataset Loader</h2>
			<h3>Create Diachronic Dataset</h3>
			<form method="POST" action="${pageContext.servletContext.contextPath}/archive/dataset">
				 <label>Label: </label>
				 <input type="text" name="label"><br /> 
				 <label>Creator: </label>
				 <input type="text" name="creator"><br />
				 <br />
				 <input type="submit" value="Create">
			</form>
			<h3>Upload new version to an existing Diachronic Dataset</h3>
			<form method="POST" enctype="multipart/form-data"
				action="${pageContext.servletContext.contextPath}/archive/dataset/version">
				<label>Diachronic URI: </label>
				<input type="text" name="DiachronicDatasetURI"><br />
				<label>	File to upload: </label>
				<input type="file" name="DataFile"><br /> 
				<br /> 
				<input type="submit" value="Upload"> 
			</form>
			<h3>Upload Quality Metadata an existing Diachronic Dataset</h3>
			<form method="POST" enctype="multipart/form-data"
				action="${pageContext.servletContext.contextPath}/archive/dataset/metadata">
				<label>Diachronic URI: </label>
				<input type="text" name="DiachronicDatasetURI"><br />
				<label>	File to upload: </label>
				<input type="file" name="DataFile"><br /> 
				<br /> 
				<input type="submit" value="Upload"> 
			</form>
			<h3>Upload Quality Metadata an existing Dataset Version</h3>
			<form method="POST" enctype="multipart/form-data"
				action="${pageContext.servletContext.contextPath}/archive/dataset/version/metadata">
				<label>Dataset Version URI: </label>
				<input type="text" name="DatasetURI"><br />
				<label>	File to upload: </label>
				<input type="file" name="DataFile"><br /> 
				<br /> 
				<input type="submit" value="Upload"> 
			</form>
		</div>
