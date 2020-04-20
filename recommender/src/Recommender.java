import entity.Item;
import entity.MatrixEntry;
import entity.Pair;
import entity.User;
import entity.UserEntry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.IntStream;

public class Recommender {

	private DataBase similarityDataBase;
    
    private HashMap<Integer, Set<UserEntry>> data;
    int numItems;
    
    
    public Recommender(HashMap<Integer, Set<UserEntry>> data) {
    	this.data = data;
    	this.numItems = data.size();
    }

    public void autoConstructSimilarityDataBase(String database) {
    	   	
        CalculateSimilarity calculator = new CalculateSimilarity();
        calculator.calculateAverageRatings(data.values());
        
        int sum = 0;
        for(int a = 1; a<=numItems; a++)
        	for(int b = a; b<=numItems; b++)
        		sum++;
    	
        int[] numArr = {0, sum};
        
        //Sorts the items in terms of their id
        ArrayList<Integer> sortedItems = new ArrayList<Integer>(data.keySet());
        Collections.sort(sortedItems);
        
        ArrayList<MatrixEntry> entries = new ArrayList<MatrixEntry>();
        
        System.out.println("IN PROGRESS: Constructing similarity matrix...");
    	IntStream.range(0,numItems).parallel().forEach(i -> {
			for(int j = i+1; j < numItems; j++) {
        		Set<UserEntry> setOne = data.get(sortedItems.get(i)); //A set of all the user entries for item i
        		Set<Integer> setOneIDs = new HashSet<>();
        		Iterator<UserEntry> itr = setOne.iterator();
        		while(itr.hasNext())
        			setOneIDs.add(itr.next().getUserID()); //Add all the user IDs for item i to a new set
        		
        		Set<UserEntry> setTwo = data.get(sortedItems.get(j)); //A set of all the user entries for item j
        		Set<Integer> setTwoIDs = new HashSet<>();
        		itr = setTwo.iterator();
        		while(itr.hasNext())
        			setTwoIDs.add(itr.next().getUserID()); //Add all the user IDs for item j to a new set
        		
        		//Gets the matching IDs which rated both items
        		setOneIDs.retainAll(setTwoIDs);
        		
        		//Make common pairs (match entries from the same user for both item i and j and put them as a pair)
        		Set<Pair> commonEntryPairs = new HashSet<>();
        		Iterator<Integer> iterator = setOneIDs.iterator();
        		while(iterator.hasNext()) {
        			UserEntry first = null;
        			UserEntry second = null;
        			Integer userID = iterator.next();
        			itr = setOne.iterator();
        			while(itr.hasNext()) {
        				UserEntry entry = itr.next();
        				if(entry.getUserID() == userID)
        					first = entry;
        			}
        			itr = setTwo.iterator();
        			while(itr.hasNext()) {
        				UserEntry entry = itr.next();
        				if(entry.getUserID() == userID)
        					second = entry;
        			}
        			Pair pair = new Pair(first, second);
        			commonEntryPairs.add(pair);
        		}
                
        		//Find the consine distance between the two items
                Double similarityScore = calculator.itemCosineDist(commonEntryPairs);
                
                if(similarityScore != null)
                	entries.add(new MatrixEntry(sortedItems.get(i), sortedItems.get(j), similarityScore));
	        }
			numArr[0] = numArr[0] + (numItems-i);
			System.out.println(numArr[0] + " / " + numArr[1] + " items complete! (" + ((double) numArr[0] / (double) numArr[1]) * 100 + "%)");
    	});
	    	
    	System.out.println("COMPLETE: Constructed similarity matrix!");
    	System.out.println("IN PROGRESS: Storing similarity matrix...");
    	
		try {
			
			DataBase db = new DataBase(database);
	    	
	    	String sql = "CREATE TABLE IF NOT EXISTS similarityTable(\n"
		            + "    item_a integer NOT NULL,\n"
		            + "    item_b integer NOT NULL,\n"
		            + "    score double NOT NULL\n"
		            + ");";
		
			db.createNewTable(sql);
	    	
			String sql2 = "INSERT INTO similarityTable(item_a, item_b, score) VALUES(?,?,?)";
	    	Connection conn = DriverManager.getConnection(db.getURL());
	        PreparedStatement pstmt = conn.prepareStatement(sql2);
	        
	        conn.setAutoCommit(false);
	    	
            int count = 0;
            int batchSize = 100;
            
	    	for(MatrixEntry entry : entries) {
	    		pstmt.setInt(1, entry.getItemA());
	            pstmt.setInt(2, entry.getItemB());
	            pstmt.setDouble(3, entry.getScore());
	            pstmt.addBatch();
	            count++;
	            if(count % batchSize == 0)
	            	pstmt.executeBatch();
	    	}
	    	
	    	pstmt.executeBatch();
	    	conn.commit();
	        conn.setAutoCommit(true);
	        
	    	System.out.println("COMPLETE: Stored similarity matrix! (" + count + " entries)");
	    	
	    	similarityDataBase = db;
    	} catch(SQLException e) {
    		System.out.println("ERROR: Adding entries into the database");
    		e.printStackTrace();
    	}
    }
    
    public void setSimilarityDataBase(DataBase db) {
    	similarityDataBase = db;
    }
    
    public Double getSimilarityScore(int itemA, int itemB) {
    	
    	if(itemA == itemB)
    		return 1.0;
    	
    	int i1 = (itemA < itemB) ? itemA : itemB;
    	int i2 = (itemB > itemA) ? itemB : itemA;
    	
    	String sql = "SELECT * FROM similarityTable WHERE item_a = " + Integer.toString(i1) + " AND item_b = " + Integer.toString(i2);
    	
    	try {
    	
    		Connection conn = DriverManager.getConnection(similarityDataBase.getURL());
	    	Statement stmt = conn.createStatement();
	    	ResultSet rs = stmt.executeQuery(sql);
	    	
	    	while (rs.next())
			    return rs.getDouble("score");
    	
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	return null;
    }


}