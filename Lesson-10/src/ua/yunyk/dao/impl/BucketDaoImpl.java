package ua.yunyk.dao.impl;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import ua.yunyk.dao.BucketDao;
import ua.yunyk.domain.Bucket;
import ua.yunyk.utils.ConnectionUtils;

public class BucketDaoImpl implements BucketDao {

	private static String READ_ALL = "select * from bucket";
	private static String CREATE = "insert into bucket(`user_id`, `product_id`, `purchase_date`) values(?,?,?)";
	private static String READ_BY_ID = "select * from bucket where id = ?";
	private static String DELETE_BY_ID = "delete from bucket where id = ?";

	private static Logger LOGGER = Logger.getLogger(UserDaoImpl.class);

	
	@Override
	public Bucket create(Bucket bucket) {
		try {
			Connection connection = ConnectionUtils.openConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(CREATE, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, bucket.getUserId());
			preparedStatement.setInt(2, bucket.getProductId());
			preparedStatement.setDate(3, new Date(bucket.getPurchaseDate().getTime()));
			preparedStatement.executeUpdate();
			ResultSet rs = preparedStatement.getGeneratedKeys();
			rs.next();
			bucket.setId(rs.getInt(1));
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException | SQLException e) {
			LOGGER.error(e);
		}
		return bucket;
	}

	@Override
	public Bucket read(Integer id) {
		Bucket bucket = null;
		try {
			Connection connection = ConnectionUtils.openConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(READ_BY_ID);
			preparedStatement.setInt(1, id);
			ResultSet resultSet = preparedStatement.executeQuery();
			resultSet.next();
			Integer userId = resultSet.getInt("user_id");
			Integer productId = resultSet.getInt("product_id");
			java.util.Date purchaseDate = resultSet.getDate("purchase_date");
			bucket = new Bucket(id, userId, productId, purchaseDate);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException | SQLException e) {
			LOGGER.error(e);
		}
		return bucket;
	}

	@Override
	public Bucket update(Bucket bucket) {
		LOGGER.error("there is no update for bucket");
		return null;
	}

	@Override
	public void delete(Integer id) {
		try {
			Connection connection = ConnectionUtils.openConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_ID);
			preparedStatement.setInt(1, id);
			preparedStatement.executeUpdate();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException | SQLException e) {
			LOGGER.error(e);
		}
	}

	@Override
	public List<Bucket> readAll() {
		List<Bucket> bucketRecords = new ArrayList<Bucket>();
		try {
			Connection connection = ConnectionUtils.openConnection();
			PreparedStatement preparedStatement = connection.prepareStatement(READ_ALL);
			ResultSet resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				Integer id = resultSet.getInt("id");
				Integer userId = resultSet.getInt("user_id");
				Integer productId = resultSet.getInt("product_id");
				java.util.Date purchaseDate = resultSet.getDate("purchase_date");
				bucketRecords.add(new Bucket(id, userId, productId, purchaseDate));
			}

		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
				| InvocationTargetException | NoSuchMethodException | SecurityException | SQLException e) {
			LOGGER.error(e);
		}
		return bucketRecords;
	}


}
