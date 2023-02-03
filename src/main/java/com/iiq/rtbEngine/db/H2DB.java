package com.iiq.rtbEngine.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;

import org.springframework.stereotype.Repository;

@Repository(value = "h2db")
public class H2DB {

	// JDBC driver name and database URL 
	static final String JDBC_DRIVER = "org.h2.Driver";   
	static final String DB_URL = "jdbc:h2:mem:test";  

	static final String USER = "user"; 
	static final String PASS = ""; 
	
	private static Connection connection; 
	
	@PostConstruct
	public void init() throws SQLException, ClassNotFoundException { 
		Class.forName(JDBC_DRIVER); 
		connection = DriverManager.getConnection(DB_URL,USER,PASS);
	}

	/**
	 * An API for executing a SQL update statement using a given SQL update statement
	 * @param statment - a SQL update statement
	 * @throws SQLException
	 */
	public void executeUpdate(String statment) throws SQLException {

		try
		{
			Statement stmt = connection.createStatement(); 
			stmt.executeUpdate(statment);
			stmt.close(); 
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}

	/**
	 * An API for executing a SQL query from a table using a given SQL query statement 
	 * @param stament - a SQL query
	 * @param columns - the names of the columns to retrieve from each of the query result rows
	 * @return a list of maps s.t. each entry in the list is equivalent to a row of the SQL query response,
	 * and each map (i.e. result row / list entry) has the given columns as the key set, and the matching values from the table 
	 * @throws Exception
	 */
	public List<Map<String, String>> executeQuery(String stament, String ... columns)  throws Exception {

		Statement createStatement = null;
		
		try
		{
			List<Map<String, String>> result = new ArrayList<>();
			createStatement = connection.createStatement();
			ResultSet rs = createStatement.executeQuery(stament);
			while (rs.next()) {
				Map<String,String> rowMap = new HashMap<>();
				result.add(rowMap);
				for(String column : columns)
					rowMap.put(column, rs.getString(column));
			}
			return result;
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		finally {
			createStatement.close();
		}
		return null;
	}

}
