package com.sri;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class MyDriver {

	/**
	 * Main function for demonstrating connecting to Cassandra with host and port.
	 *
	 * @param args Command-line arguments; first argument, if provided, is the
	 *    host and second argument, if provided, is the port.
	 */
	public static void main(final String[] args)
	{
	   final CassandraConnector client = new CassandraConnector();
	   final String ipAddress = args.length > 0 ? args[0] : "localhost";
	   final int port = args.length > 1 ? Integer.parseInt(args[1]) : 9042;
	   System.out.println("Connecting to IP Address " + ipAddress + ":" + port + "...");
	   client.connect(ipAddress, port);
	   insertUser(client.getSession(),600,"Sita","Krishnan");
	   
	   //Fetch the inserted data
	   getAllRecords(client.getSession());
	   getByUserId(client.getSession(),600);
	   
	 //  getByLastName(client.getSession(),600);
	   
	   client.close();
	}

	
	public static void insertUser(Session session,int user_id,String fname,String lname){

		System.out.println("Inserting User...");
		PreparedStatement statement = session.prepare(
				 
				"INSERT INTO users" + "(user_id,fname, lname)"
						+ "VALUES (?,?,?);");
		 
				BoundStatement boundStatement = new BoundStatement(statement);
		 
				session.execute(boundStatement.bind(user_id,fname,lname));
		
	}
	
	
	public static void getAllRecords(Session session){
		System.out.println("Inside getRecords...");
		Statement statement = QueryBuilder.select()
		        .all()
		        .from("myspace", "users");
		
		ResultSet results = session.execute(statement);
		for (Row row : results) {
			   System.out.format("%s %s\n", row.getString("fname"), row.getString("lname"));
			}
	}
	
	
	public static void getByUserId(Session session,int user_id){
		System.out.println("Inside getByUserId...");
		Statement select=QueryBuilder.select()
        .all()
        .from("myspace", "users")
		.where(QueryBuilder.eq("user_id", user_id));
		
		
		ResultSet results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %s \n", row.getString("fname"),
					row.getString("lname"));
		}
	}
	
	
	public static void getByLastName(Session session,int user_id){
		System.out.println("Inside getByLastName...");
		Statement select=QueryBuilder.select()
        .all()
        .from("myspace", "users")
        //.where(eq("lname", lname));
		.where(QueryBuilder.eq("user_id", user_id));
		
		
		ResultSet results = session.execute(select);
		for (Row row : results) {
			System.out.format("%s %d \n", row.getString("fname"),
					row.getString("lname"));
		}
	}
	
	
}
