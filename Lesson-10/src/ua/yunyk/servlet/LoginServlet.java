package ua.yunyk.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.google.gson.Gson;

import ua.yunyk.domain.User;
import ua.yunyk.dto.UserLogin;
import ua.yunyk.service.UserService;
import ua.yunyk.service.impl.UserServiceImpl;

@WebServlet(name = "Login", urlPatterns = { "/Login" })
public class LoginServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		UserService userService = UserServiceImpl.getUserService();
		String email = request.getParameter("email");
		String password = request.getParameter("password");	
		User user = userService.getUserByEmail(email);
		if (user == null) {
			request.setAttribute("errorMessage",
					"<br>There is no users with entered email<br> <a href=\"Registration.jsp\">Registration</a>");
			request.setAttribute("email", "e");
			request.getRequestDispatcher("login.jsp").forward(request, response);
		} else if (!(user.getPassword().equals(password))) {
			request.setAttribute("email", user.getEmail());
			request.setAttribute("errorMessage",
					"<br>Password entered wrong <br> <a href=\"Registration.jsp\">Registration</a>");
			request.getRequestDispatcher("login.jsp").forward(request, response);
		} else {			
			HttpSession session = request.getSession(true);
			session.setAttribute("userId", user.getId());
			session.setAttribute("role", user.getRole().toString());
			
			UserLogin userLogin = new UserLogin();
			
			userLogin.destinationUrl = "cabinet.jsp";
			userLogin.userEmail = user.getEmail();
			String json = new Gson().toJson(userLogin);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().write(json);
		}

	}

}
