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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import entity.User;
import entity.UserEntry;
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
	
	public boolean tableExists(String name) {
        try (Connection conn = DriverManager.getConnection(url);
        		Statement stmt = conn.createStatement()) {
            // Check if table exists
        	
        	DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, name, null);
            return rs.next();
            
        } catch (SQLException e) {
            return false;
        }
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
            
			br = new BufferedReader(new FileReader(file));
			int lineno = 1;
	        while ((line = br.readLine()) != null) {
	        	String[] table = line.split(",");
	        	try {
		            pstmt.setInt(1, Integer.valueOf(table[0]));
		            pstmt.setInt(2, Integer.valueOf(table[1]));
		            pstmt.setDouble(3, Double.valueOf(table[2]));
		            pstmt.setLong(4, Long.valueOf(table[3]));
		        	pstmt.addBatch();
		        	
		        	count++;
	        	} catch(NumberFormatException e) {
	        		System.out.println("ERROR: Entry on line " + lineno + " is of an invalid format!");
	        	}
	        	lineno++;
	        	
	        	if(count % batchSize == 0) {
	                pstmt.executeBatch();
	        	}
	        	
	        }
	        pstmt.executeBatch();
	        conn.commit();
	        conn.setAutoCommit(true);
	        System.out.println("COMPLETE: " + count + " entries were added to the database!");
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /*
     * DECRECATED METHOD - DO NOT USE
     */
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
    
    /*
     * DECRECATED METHOD - DO NOT USE
     */
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
    
    public HashMap<Integer, Set<UserEntry>> contructItemsHash() {
    	
    	HashMap<Integer, Set<UserEntry>> entries = new HashMap<Integer, Set<UserEntry>>();
    	
    	try {
        	Connection conn = DriverManager.getConnection(url);
        	Statement stmt = conn.createStatement();
        	ResultSet rs = stmt.executeQuery("SELECT DISTINCT itemid FROM userRatings");
        	
        	ArrayList<Integer> items = new ArrayList<Integer>();
        	while (rs.next()) {
    		    int id = rs.getInt("itemid");
    		    items.add(id);
        	}
        	
        	int count = 0;
        	for(Integer itemID : items) {
        		ResultSet entry = stmt.executeQuery("SELECT userid, rating, timestamp FROM userRatings WHERE itemid='" + Integer.toString(itemID) + "'");
        		Set<UserEntry> userEntries = new HashSet<>();
        		while(entry.next()) {
        			int item = entry.getInt("userid");
        			float rating = entry.getFloat("rating");
        			int timestamp = entry.getInt("timestamp");
        			UserEntry ue = new UserEntry(item, rating, timestamp);
        			userEntries.add(ue);
        		}
        		count++;
        		if(count == items.size() || count % 1000 == 0)
        			System.out.println(count + "/" + items.size() + " completed! (" + ((double) count / (double) items.size()) * 100 + "%)");
        		entries.put(itemID, userEntries);
        	}
 
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	return entries;
    }
    
    public String getURL() {
    	return url;
    }
    
}
