import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import entity.User;
import entity.Item;

public class DataBase {
	
	String fileName;
	String url;
	
	boolean alreadyExisted;
	
	DataBase(String fileName) {
		this.fileName = fileName;
		this.url = "jdbc:sqlite:" + fileName;
		
		File file = new File(fileName);
		
		alreadyExisted = file.exists();
		if(!alreadyExisted) {
	        try (Connection conn = DriverManager.getConnection(url)) {
	            if (conn != null) {
	                DatabaseMetaData meta = conn.getMetaData();
	                System.out.println("The driver name is " + meta.getDriverName());
	                System.out.println("A new database has been created under the name: " + fileName);
	            }
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
		}
	}
	
	public boolean exists() {
		return alreadyExisted;
	}
	
    public void createNewTable(String sql) {
        // SQLite connection string
        try (Connection conn = DriverManager.getConnection(url);
        		Statement stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
            System.out.println("A new table has been created!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    
    public void batchInsert(String file) {
		BufferedReader br = null;
	    String line;
	    
	    //USE BATCH INSERT FOR EFFICIENCY!
	    System.out.println("IN PROGRESS: Inserting data into the database...");
	    try {
	    	String sql = "INSERT INTO userRatings(userid,itemid,rating,timestamp) VALUES(?,?,?,?)";
	    	Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            conn.setAutoCommit(false);
            
            int count = 0;
            int batchSize = 100;

            int[] result;
            
			br = new BufferedReader(new FileReader(file));
			int lineno = 1;
	        while ((line = br.readLine()) != null) {
	        	String[] table = line.split(",");
	        	try {
		            pstmt.setInt(1, Integer.valueOf(table[0]));
		            pstmt.setInt(2, Integer.valueOf(table[1]));
		            pstmt.setDouble(3, Double.valueOf(table[2]));
		            pstmt.setInt(4, Integer.valueOf(table[3]));
		        	pstmt.addBatch();
	        	} catch(NumberFormatException e) {
	        		System.out.println("ERROR: Entry on line " + lineno + " is of an invalid format!");
	        	}
	        	lineno++;
	        	count++;
	        	
	        	if(count % batchSize == 0) {
	                result = pstmt.executeBatch();
	                System.out.println(count);
	        	}
	        	
	        }
	        result = pstmt.executeBatch();
	        conn.commit();
	        conn.setAutoCommit(true);
	        System.out.println("COMPLETE: " + result.length + " entries were added to the database!");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public ArrayList<Item> contructItems() {
    	
    	ArrayList<Item> items = new ArrayList<Item>();
    	
    	try {
        	Connection conn = DriverManager.getConnection(url);
        	Statement stmt = conn.createStatement();
        	ResultSet rs = stmt.executeQuery("SELECT DISTINCT itemid FROM userRatings");
        	
        	while (rs.next()) {
    		    int id = rs.getInt("itemid");
    		    items.add(new Item(id));
        	}
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	return items;
    }
    
    public Set<User> constructUsers() {
    	
    	Set<User> users = new HashSet<>();
    	
    	try {
        	Connection conn = DriverManager.getConnection(url);
        	Statement stmt = conn.createStatement();
        	ResultSet rs = stmt.executeQuery("SELECT DISTINCT userid FROM userRatings");
        	
        	while (rs.next()) {
    		    int userid = rs.getInt("userid");
    		    User user = new User(userid);
    		    users.add(user);
        	}
 
        	for(User user : users) {
        		ResultSet itemRatings = stmt.executeQuery("SELECT itemid, rating FROM userRatings WHERE userid='" + Integer.toString(user.getUserId()) + "'");
        		while(itemRatings.next()) {
        			int item = itemRatings.getInt("itemid");
        			double rating = rs.getDouble("rating");
        			user.rate(new Item(item), rating);
        		}
        	}
        	
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	return users;
    }
    
}
