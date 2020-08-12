package ua.yunyk.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import ua.yunyk.domain.User;
import ua.yunyk.domain.UserRole;
import ua.yunyk.service.UserService;
import ua.yunyk.service.impl.UserServiceImpl;

@WebServlet(asyncSupported = true, name = "registration", urlPatterns = { "/registration" })
public class RegistrationServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		UserService userService = UserServiceImpl.getUserService();
		User user = new User(request.getParameter("email"), request.getParameter("password"),
				request.getParameter("firstName"), request.getParameter("lastName"), UserRole.USER.toString());
		
		user = userService.create(user);
		if (user.getId() != null) {
			response.setContentType("text/plain");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().write("Success");
		} else {
			request.setAttribute("errorMessage",
					"<br>Registration failed. Please check your network connection <br>");
			request.setAttribute("firstName", user.getFirstName());
			request.setAttribute("lastName", user.getLastName());
			request.setAttribute("email", user.getEmail());
			request.getRequestDispatcher("Registration.jsp").forward(request, response);
		}

	}

}
