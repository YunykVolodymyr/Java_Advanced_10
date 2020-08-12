package ua.yunyk.service;

import java.util.Map;

import ua.yunyk.domain.Product;
import ua.yunyk.shared.AbstractCRUD;

public interface ProductService extends AbstractCRUD<Product>{
	public Map<Integer, Product> readAllMap();
}
