import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import entity.UserEntry;

public class Validator {
	
	private SimilarityMatrix similarityMatrix;
	private HashMap<Integer, HashMap<Integer, UserEntry>> train;
	private HashMap<Integer, HashMap<Integer, UserEntry>> validate;
	
	public Validator(HashMap<Integer, HashMap<Integer, UserEntry>> data, double percentage) {
		
		train = new HashMap<Integer, HashMap<Integer, UserEntry>>();
		validate = new HashMap<Integer, HashMap<Integer, UserEntry>>();
		
		for(Entry<Integer, HashMap<Integer, UserEntry>> entry : data.entrySet()) {
			//The set of user entries for this item
			Collection<UserEntry> set = entry.getValue().values();
			//The amount that are needed to train, the rest is used to validate
			int trainCount = (int) (set.size() * percentage);
			//Makes a temporary training set and validating set
			HashMap<Integer, UserEntry> trainSet = new HashMap<Integer, UserEntry>();
			HashMap<Integer, UserEntry> validateSet = new HashMap<Integer, UserEntry>();
			//Loops through user entries, adding to either training or validating set depending on the count
			int count = 0;
			for(UserEntry userEntry : set) {
				if(count <= trainCount) {
					trainSet.put(userEntry.getUserID(), userEntry);
					count++;
				} else {
					validateSet.put(userEntry.getUserID(), userEntry);
				}
			}
			train.put(entry.getKey(), trainSet);
			validate.put(entry.getKey(), validateSet);
		}
		
		similarityMatrix = new SimilarityMatrix("ValidatorSimilarityMatrix.db", train);
	}
	
	public double getPredictionScore() {
		
		Predictor predictor = new Predictor(train, similarityMatrix.getDataBase());
		
		int total = 0;
		int correct = 0;
		
		double mse = 0.0;
		
		int count = 0;
		for(Entry<Integer, HashMap<Integer, UserEntry>> entry : validate.entrySet()) {
			
			LinkedHashMap<Integer, Double> similarItems = new LinkedHashMap<>();
			try {
				Connection conn = DriverManager.getConnection(similarityMatrix.getDataBase().getURL());
	    		Statement stmt = conn.createStatement();
				
				String sql = "SELECT * FROM similarityTable WHERE item_a = " + entry.getKey() + " OR item_b = " + entry.getKey() + " AND score > 0.0";
				
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					int a = rs.getInt("item_a");
					int b = rs.getInt("item_b");
					similarItems.put((a == entry.getKey()) ? b : a, rs.getDouble("score"));
				}
			} catch(SQLException e) {
				e.printStackTrace();
			}
			
			for(UserEntry user : entry.getValue().values()) {
				double prediction = predictor.predict(1000, entry.getKey(), user.getUserID(), similarItems);
				if(prediction == (double) user.getRating())
					correct++;
				total++;
				
				mse += Math.pow((prediction - (double) user.getRating()), 2);
			}
			count++;
			System.out.println(count + "/" + validate.size() + " items complete! (" + (count / (double)validate.size()) * 100 + "%)");
		}
		
		mse /= total;
		System.out.println("MSE: " + mse);
		
		return correct / (double) total; 
	}

}
