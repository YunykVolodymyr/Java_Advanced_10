<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<!DOCTYPE html>
<html>
<head>
<style type="text/css">
html, body {
	height: 100%;
	width: 100%;
	margin: 0;
}

body {
	display: flex;
	background-color: rgba(238,238,238);
}

form {
	height: 250px;
	width: 200px;
	margin: auto;
	text-align: center;
	background-color: white;
	border-radius: 25px;
}

input {
	margin-top: 10px;
}

.error{
	margin-top: 40px;
}
</style>

<meta charset="ISO-8859-1">
<title>Insert title here</title>
</head>
<body>
	<form name="form" action="registration" method="post">
		<label for="form">Registration</label>
		<input name="firstName" type="text" placeholder="First Name" value="${firstName}">
		<input name="lastName" type="text" placeholder="Last Name" value="${lastName}">
		<input name="email" type="email" placeholder="email" value="${email}"> 
		<input name="password" type="password" placeholder="password"> 
		<input type="reset" value="Reset"> 
		<input type="submit" value="Register">

		<label class="error"> ${errorMessage} </label>

	</form>

</body>
</html>