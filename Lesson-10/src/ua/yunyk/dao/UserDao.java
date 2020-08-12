package ua.yunyk.dao;

import ua.yunyk.domain.User;
import ua.yunyk.shared.AbstractCRUD;

public interface UserDao extends AbstractCRUD<User>{
	
	User getUserByEmail(String email);
	
}
