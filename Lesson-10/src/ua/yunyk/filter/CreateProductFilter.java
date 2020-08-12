package ua.yunyk.filter;

import java.io.IOException;
import java.util.Arrays;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebFilter;

import ua.yunyk.domain.UserRole;
import ua.yunyk.shared.FilterService;


@WebFilter("/createProduct.jsp")
public class CreateProductFilter implements Filter {

FilterService filterService = FilterService.getFilterService();
	
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		filterService.doFilterServiceValidation(request, response, chain, Arrays.asList(UserRole.ADMINISTRATOR));
	}

	
	public void init(FilterConfig fConfig) throws ServletException {}
}
