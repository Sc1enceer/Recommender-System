import entity.UserEntry;
import java.util.HashMap;

public class Recommender {
    
	private SimilarityMatrix similarityMatrix;
    private HashMap<Integer, HashMap<Integer, UserEntry>> data;
    
	public Recommender(DataBase userEntries) {
		System.out.println("IN PROGRESS: Retrieving items and users from the database...");
		data = userEntries.contructItemsHash();
		System.out.println("COMPLETE: Retrieved items and users from the database!");
    		
    	similarityMatrix = new SimilarityMatrix("SimilarityMatrix.db", data);
    		
    	//Creates a validator with the data and sets 90% training and 10% validating
    	//validator = new Validator(data, 0.9);
    	//System.out.println("Validator Score: " + (validator.getPredictionScore()*100.0) + "%");
    		
    	Predictor predictor = new Predictor(data, similarityMatrix.getDataBase());
    	predictor.predictAll(100, "comp3208-test.csv", "output.csv");
    }
 
}