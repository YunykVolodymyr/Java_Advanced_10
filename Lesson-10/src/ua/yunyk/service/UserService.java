package ua.yunyk.service;

import ua.yunyk.domain.User;
import ua.yunyk.shared.AbstractCRUD;

public interface UserService extends AbstractCRUD<User>{
	
	User getUserByEmail(String email);
	
}
