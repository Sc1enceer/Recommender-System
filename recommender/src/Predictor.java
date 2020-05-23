import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import entity.UserEntry;

public class Predictor {
	private HashMap<Integer, HashMap<Integer, UserEntry>> userEntries;
	private DataBase similarityDataBase;
	
	private HashMap<Integer, ArrayList<Integer>> usersToRatedItems; //A map from user to a list of items that the user rated
	
	private HashMap<Integer, Double> itemsToAverageRatings;
	
	public Predictor(HashMap<Integer, HashMap<Integer, UserEntry>> userEntries, DataBase similarityDataBase) {
		this.userEntries = userEntries;
		this.similarityDataBase = similarityDataBase;
		
		usersToRatedItems = new HashMap<>();
		itemsToAverageRatings = new HashMap<>();
		
    	for(Entry<Integer, HashMap<Integer, UserEntry>> entry : userEntries.entrySet()) {
    		double total = 0.0;
    		for(UserEntry user : entry.getValue().values()) {
    			ArrayList<Integer> itemsRated;
    			if(usersToRatedItems.containsKey(user.getUserID())) {
    				itemsRated = usersToRatedItems.get(user.getUserID());
    			} else {
    				itemsRated = new ArrayList<Integer>();
    			}
    			itemsRated.add(entry.getKey());
    			usersToRatedItems.put(user.getUserID(), itemsRated);
    			total += user.getRating();
    		}
    		itemsToAverageRatings.put(entry.getKey(), total/entry.getValue().size());
    	}
	}

	public Double getRating(Integer userID, Integer itemID) {
		HashMap<Integer, UserEntry> set = userEntries.get(itemID);
		for(UserEntry entry : set.values()) {
			if(entry.getUserID() == userID) {
				return entry.getRating();
			}
		}
		return null;
	}
	
	public Double predict(Integer k, Integer itemID, Integer userID, LinkedHashMap<Integer, Double> similarItems) {
		
		LinkedHashMap<Integer, Double> itemMap = new LinkedHashMap<>();
		
		if(similarItems != null) {
		
			int count = 0;
			for(Entry<Integer, Double> entry : similarItems.entrySet()) {
				if(count < k) {
					if(usersToRatedItems.get(userID) != null && usersToRatedItems.get(userID).contains(entry.getKey())) {
						itemMap.put(entry.getKey(), entry.getValue());
						count++;
					}
				} else {
					break;
				}
			}
	    	
	    	//itemMap = sortByValue(itemMap);
	    	
	    	double nom = 0.0;
	    	double denom = 0.0;
	  
	    	Iterator<Integer> iter = itemMap.keySet().iterator();
	    	while(iter.hasNext()) {
	    		
	    		Integer tempItemId = iter.next();
	    	
	    		double sim = itemMap.get(tempItemId);
	    		double userRating = getRating(userID, tempItemId);
	    		
	    		nom += sim * userRating;
	    		denom += sim;
	    	}
	    	
	    	if(itemMap.isEmpty())
	    		return (itemsToAverageRatings.containsKey(itemID) ? itemsToAverageRatings.get(itemID) : 3.0);
	    	
			return (nom/denom);
		}
		return (itemsToAverageRatings.containsKey(itemID) ? itemsToAverageRatings.get(itemID) : 3.0);
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
                return (o2.getValue()).compareTo(o1.getValue()); 
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
		
		HashMap<Integer, ArrayList<Integer>> itemsToUsers = new HashMap<>();
		
		int linesTotal = 0;
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			
			String line;
			while ((line = reader.readLine()) != null) {
				String[] table = line.split(",");
				int userID = Integer.valueOf(table[0]);
				int itemID = Integer.valueOf(table[1]);
				
				ArrayList<Integer> temp;
				if(itemsToUsers.containsKey(itemID)) {
					temp = itemsToUsers.get(itemID);
				} else {
					temp = new ArrayList<>();
				}
				temp.add(userID);
				itemsToUsers.put(itemID, temp);
				
				linesTotal++;
			}
			
			reader.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
			
		boolean complete = false;
		
		do {
			
			try {
				
				ArrayList<String> outputs = new ArrayList<String>();
				
				File out = new File(outputFile);
				int linesDone = 0;
				if(out.exists()) {
					BufferedReader reader = new BufferedReader(new FileReader(outputFile));
					String line;
					while ((line = reader.readLine()) != null) linesDone++;
					reader.close();
				}
				
				String sql = "SELECT * FROM similarityTable WHERE (item_a = ? OR item_b = ?) AND score > 0 order by score desc";
				
				Connection conn = DriverManager.getConnection(similarityDataBase.getURL());
				PreparedStatement pstmt = conn.prepareStatement(sql);
			
				int done = 0;
				for(Entry<Integer, ArrayList<Integer>> entry : itemsToUsers.entrySet()) {
					
					int itemID = entry.getKey();
					LinkedHashMap<Integer, Double> similarItems = new LinkedHashMap<>();
					
					boolean set = false;
					while(!set) {
						try {
							pstmt.setInt(1, itemID);
							pstmt.setInt(2, itemID);
							ResultSet rs = pstmt.executeQuery();
							
							while (rs.next()) {
								int a = rs.getInt("item_a");
								int b = rs.getInt("item_b");
								similarItems.put((a == itemID) ? b : a, rs.getDouble("score"));
							}
							
							set = true;
						} catch(Throwable t) {
							System.out.println("ERROR: An exception occured whilst finding similar items!");
			    			conn.close();
			    			pstmt.close();
			    			conn = DriverManager.getConnection(similarityDataBase.getURL());
			    	        pstmt = conn.prepareStatement(sql);
			    	        similarItems.clear();
						}
					
					}
					
					for(Integer userID : entry.getValue()) {
						
						if(done >= linesDone) {
							Double rating = predict(k, itemID, userID, similarItems);
							outputs.add(Integer.toString(userID) + "," + Integer.toString(itemID) + "," + Double.toString(rating) + "\n");
						}
						
						done++;
						System.out.println(done + " / " + linesTotal + " completed! (" + ((done / (double)linesTotal)*100) + "%)");
						
						if(done % 10000 == 0) {
							FileWriter writer = new FileWriter(outputFile, true);
							BufferedWriter bw = new BufferedWriter(writer);
							
							for(int i = outputs.size()-1; i>=0; i--) {
								bw.write(outputs.get(i));
								outputs.remove(i);
							}
							
							bw.close();
							writer.close();
						}
					}
				}
				
				FileWriter writer = new FileWriter(outputFile, true);
				BufferedWriter bw = new BufferedWriter(writer);
				
				for(int i = outputs.size()-1; i>=0; i--) {
					bw.write(outputs.get(i));
					outputs.remove(i);
				}
				
				bw.close();
				writer.close();
				
				complete = true;
			
			} catch(Exception e) {
				e.printStackTrace();
			}
		
		} while(!complete);
	}

}
