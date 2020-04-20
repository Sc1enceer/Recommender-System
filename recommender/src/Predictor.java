import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import entity.UserEntry;

public class Predictor {
	private DataBase similarityMatrix;
	private DataBase userRatings;
	private HashMap<Integer, Set<UserEntry>> userEntries;
	
	
	public Predictor(DataBase similarityMatrix, DataBase userRatings,  HashMap<Integer, Set<UserEntry>> userEntries) {
		this.similarityMatrix = similarityMatrix;
		this.userRatings = userRatings;
		this.userEntries = userEntries;
	}
	
	public double getRating(Integer userId, Integer itemId) {
		String sql = "SELECT * FROM UserRatings WHERE userid = " + Integer.toString(userId) + " AND itemid = " + Integer.toString(itemId);
    	
    	try {
    	
    		Connection conn = DriverManager.getConnection(userRatings.getURL());
	    	Statement stmt = conn.createStatement();
	    	ResultSet rs = stmt.executeQuery(sql);
	    	
	    	while (rs.next())
	    		return rs.getDouble("rating");
	    	
		} catch(Exception e) {
			e.printStackTrace();
		}
    	
    	return 0.0;
	}
	
	
	public Double predict(Integer k, Integer userID, Integer itemId) {
		
		HashMap<Integer, Double> itemMap = new HashMap<>();
		String sql = "SELECT * FROM similarityMatrix WHERE item_a = " + Integer.toString(itemId) + " OR item_b = " + Integer.toString(itemId);
    	
    	try {
    	
    		Connection conn = DriverManager.getConnection(similarityMatrix.getURL());
	    	Statement stmt = conn.createStatement();
	    	ResultSet rs = stmt.executeQuery(sql);
	    	
	    	while (rs.next()) {
	    		Integer tempId1 = rs.getInt("item_a");
	    		Integer tempId2 = rs.getInt("item_b");
	    		if(rs.getDouble("score") > 0)
		    		itemMap.put(((tempId1 == itemId) ? tempId2 : tempId1), rs.getDouble("score"));
	    	}
	    	
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}	
    	
    	//Loops through all entries, see if the user has rated that item before, if not, remove from the map
    	for(Entry<Integer, Double> entry : itemMap.entrySet()) {
    		Set<UserEntry> contents = userEntries.get(entry.getKey());
    		boolean found = false;
    		for(UserEntry user : contents) {
    			if(user.getUserID() == userID) {
    				found = true;
    				break;	
    			}
    		}
    		if(!found)
    			itemMap.remove(entry);
    	}	    	    	
    	
    	itemMap = sortByValue(itemMap);
    	double nom = 0.0;
    	double denom = 0.0;

    	Iterator<Integer> iter = itemMap.keySet().iterator();
    	int count = 0;
    	while(iter.hasNext() && count < k) {
    		
    		Integer tempItemId = iter.next();
    	
    		double sim = itemMap.get(tempItemId);
    		double userRating = getRating(userID,tempItemId );
    		nom += sim * userRating;
    		denom += sim;
    	}	
    	
		return Math.round((nom/denom) * 2) / 2.0;
	}
	
	
	public static HashMap<Integer, Double> sortByValue(HashMap<Integer, Double> hm) 
    { 
        // Create a list from elements of HashMap 
        List<Map.Entry<Integer, Double> > list = 
               new LinkedList<Map.Entry<Integer, Double> >(hm.entrySet()); 
  
        // Sort the list 
        Collections.sort(list, new Comparator<Map.Entry<Integer, Double> >() { 
            public int compare(Map.Entry<Integer, Double> o1,  
                               Map.Entry<Integer, Double> o2) 
            { 
                return (o1.getValue()).compareTo(o2.getValue()); 
            } 
        }); 
          
        // put data from sorted list to hash map  
        HashMap<Integer, Double> temp = new LinkedHashMap<Integer, Double>(); 
        for (Map.Entry<Integer, Double> aa : list) { 
            temp.put(aa.getKey(), aa.getValue()); 
        } 
        return temp; 
    }
	
	public void predictAll(Integer k, String inputFile, String outputFile) {
		 try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			FileWriter writer = new FileWriter(outputFile);
			BufferedWriter bw = new BufferedWriter(writer);
			
			String line; 
			
			while ((line = br.readLine()) != null) {
				String[] table = line.split(",");
				Double rating = predict(k, Integer.valueOf(table[0]), Integer.valueOf(table[1]));
				bw.write(table[0] + ',' + table[1] + ',' + Double.toString(rating));
			}
			
			bw.close();
			writer.close();
			br.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
