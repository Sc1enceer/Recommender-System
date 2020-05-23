import entity.Pair;
import entity.UserEntry;
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
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.Set;

public class SimilarityMatrix {
	
	private DataBase similarityDataBase = null;
	private String databaseName;
	private HashMap<Integer, HashMap<Integer, UserEntry>> data;
	int numItems;
	
	HashMap<Integer, HashMap<Integer, Double>> similarityItems;
	
	public SimilarityMatrix(String databaseName, HashMap<Integer, HashMap<Integer, UserEntry>> data) {
		this.databaseName = databaseName;
		this.data = data;
		this.numItems = data.size();
		this.similarityItems = new HashMap<Integer, HashMap<Integer, Double>>();
		
		similarityDataBase = constructSimilarityMatrix();
	}
	
	private DataBase constructSimilarityMatrix() {
		DataBase db = new DataBase(databaseName);
		
		HashMap<Integer, Double> averageRatings = calculateAverageRatings();
        
        ArrayList<Integer> alreadyAddedItems = getAddedItems(db);
        
	        if(alreadyAddedItems.size() < numItems) {
	        
	        int sum = 0;
	        for(int a = 0; a<numItems; a++)
	        	for(int b = a+1; b<numItems; b++)
	        		sum++;
	    	
	        int[] numArr = {0, sum};
	        
	        //Sorts the items in terms of their id
	        ArrayList<Integer> sortedItems = new ArrayList<Integer>(data.keySet());
	        Collections.sort(sortedItems);
	        
	        try {
	        
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
	        
		        System.out.println("IN PROGRESS: Constructing and saving similarity matrix...");
		        
		        for(int i = 0; i < numItems; i++) {
		        	
		        	int itemA = sortedItems.get(i);
			    		
		    		HashMap<Integer, Double> mappings = new HashMap<Integer, Double>();
		    		
		    		if(!alreadyAddedItems.contains(itemA)) {
		    		
		    			IntStream.range(i+1,numItems).parallel().forEach(j -> {
		    				boolean notSatisfied = true;
		    				while(notSatisfied) {
			    				try {
			    				
									int itemB = sortedItems.get(j);
									
									HashMap<Integer, UserEntry> setOne = data.get(itemA);
									HashMap<Integer, UserEntry> setTwo = data.get(itemB);
									
									ArrayList<Integer> setOneIDs = new ArrayList<>(setOne.keySet());
									ArrayList<Integer> setTwoIDs = new ArrayList<>(setTwo.keySet());
									
									setOneIDs.retainAll(setTwoIDs);
									
									Set<Pair> commonEntryPairs = new HashSet<>();
									for(Integer retainedID : setOneIDs)
										commonEntryPairs.add(new Pair(setOne.get(retainedID), setTwo.get(retainedID)));
					                
					        		//Find the consine distance between the two items
					                Double similarityScore = itemCosineDist(commonEntryPairs, averageRatings);
					               
					                if(similarityScore != null)
					                	mappings.put(itemB, similarityScore);
									
									notSatisfied = false;
						                
								} catch (Exception e) {
									System.out.println("ERROR: An exception occured whilst creating the batch!");
								}
		    				}
							
				        });
	    			
		    		}
	    			
		    		boolean batchAdded = false;
		    		
		    		do {
		    		
		    		try {
		    			
		    			for(Entry<Integer, Double> entry : mappings.entrySet()) {
		            		pstmt.setInt(1, itemA);
		            		pstmt.setInt(2, entry.getKey());
		            		pstmt.setDouble(3, entry.getValue());
		            		pstmt.addBatch();
		                }
		    			if(!mappings.isEmpty()) {
		    				pstmt.executeBatch();
		    				conn.commit();
		    				batchAdded = true;
		    			} else {
		    				batchAdded = true;
		    			}
	    			
		    		} catch(Throwable t) {
		    			System.out.println("ERROR: An exception occured whilst adding the batch!");
		    			conn.close();
		    			pstmt.close();
		    			conn = DriverManager.getConnection(db.getURL());
		    	        pstmt = conn.prepareStatement(sql2);
		    	        conn.setAutoCommit(false);
		    		}
	    			
		    		} while(!batchAdded);
					
					numArr[0] = numArr[0] + (numItems-i);
					System.out.println(numArr[0] + " / " + numArr[1] + " items complete! (" + ((double) numArr[0] / (double) numArr[1]) * 100 + "%)");
		        }
		    	
		        conn.setAutoCommit(true);
	    	
	        } catch (SQLException e) {
	        	e.printStackTrace();
	        }
        
        }
	    	
    	System.out.println("COMPLETE: Constructed and saved similarity matrix!");
    	
    	return db;
	}
	
    public HashMap<Integer, Double> calculateAverageRatings() {
		
    	HashMap<Integer, Accumulator> acc = new HashMap<>();
    	
    	Iterator<HashMap<Integer, UserEntry>> iterator = data.values().iterator();
    	while(iterator.hasNext()) {
    		HashMap<Integer, UserEntry> entry = iterator.next();
    		for(Entry<Integer, UserEntry> userEntry : entry.entrySet()) {
    			if(acc.containsKey(userEntry.getKey())) {
    				acc.put(userEntry.getKey(), acc.get(userEntry.getKey()).addtoAccumulator(userEntry.getValue().getRating()));
    			} else {
    				acc.put(userEntry.getKey(), new Accumulator(userEntry.getValue().getRating()));
    			}
    		}
    	}
    	
    	HashMap<Integer, Double> averages = new HashMap<>();
    	for(Entry<Integer, Accumulator> entry : acc.entrySet()) {
    		averages.put(entry.getKey(), entry.getValue().getAverage());
    	}
    	
    	return averages;
    }
    
    private class Accumulator {
    	
   		private double sum;
   		private int total;
   		
   		Accumulator(double sum) {
			this.sum = sum;
			this.total = 1;
		}
   		
   		public Accumulator addtoAccumulator(double addTo) {
   			sum += addTo;
   			total++;
   			return this;
   		}
   		
   		public double getAverage() {
   			return sum / total;
   		}
	}
    
    private Double itemCosineDist(Set<Pair> entries, HashMap<Integer, Double> averageRatings) { //Will find the cosine distance between two items
    	
    	if(entries.isEmpty())
    		return null;
    	
    	double sumTop = 0.0;
    	double sumBotL = 0.0;
    	double sumBotR = 0.0;
    	
    	Iterator<Pair> iterator = entries.iterator();
    	while(iterator.hasNext()) {
    		Pair pair = iterator.next();
    		sumTop += ((pair.getFirst().getRating() - averageRatings.get(pair.getFirst().getUserID()))*(pair.getSecond().getRating() - averageRatings.get(pair.getSecond().getUserID())));
    		sumBotL += Math.pow((pair.getFirst().getRating() - averageRatings.get(pair.getFirst().getUserID())), 2);
    		sumBotR += Math.pow((pair.getSecond().getRating() - averageRatings.get(pair.getSecond().getUserID())), 2);
    	}
    	
    	if(sumTop == 0)
    		return 0.0;
    	if((Math.sqrt(sumBotL) * Math.sqrt(sumBotR)) == 0)
    		return 1.0; //Not sure if we should return 1 if the denom is 0

		return (sumTop / (Math.sqrt(sumBotL) * Math.sqrt(sumBotR)));
    }
    
    public DataBase getDataBase() {
    	return similarityDataBase;
    }
    
    public ArrayList<Integer> getAddedItems(DataBase db) {
		
    	ArrayList<Integer> items = new ArrayList<Integer>();
    	
    	try {
    		
    		Connection conn = DriverManager.getConnection(db.getURL());
    		Statement stmt = conn.createStatement();
    		
			String sql = "SELECT DISTINCT item_a FROM similarityTable";
			ResultSet rs = stmt.executeQuery(sql);
			
			while (rs.next())
				items.add(rs.getInt("item_a"));
			
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	return items;
    	
    }
    
    public void createDataStruct(Set<Integer> items) {
    	
		System.out.println("IN PROGRESS: Getting the similarity matrix data structure...");
		
    	try {
    		
    		Connection conn = DriverManager.getConnection(similarityDataBase.getURL());
    		Statement stmt = conn.createStatement();

    		int done = 0;
			for(Integer item : items) {
				String sql = "SELECT * FROM similarityTable WHERE item_a = " + item + " AND score > 0.0";
				
				HashMap<Integer, Double> temp = new HashMap<>();
				
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next())
					temp.put(rs.getInt("item_b"), rs.getDouble("score"));
				similarityItems.put(item, temp);
				
				done++;
				System.out.println(done + " / " + items.size());
			}
	    	
    	} catch(SQLException e) {
    		e.printStackTrace();
    	}
    	
    	System.out.println("COMPLETE: Obtained the similarity matrix data structure!");
    }
    
    public HashMap<Integer, HashMap<Integer, Double>> getDataStruct() {
    	return similarityItems;
    }
    
}