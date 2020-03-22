import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Set;

import entity.Item;
import entity.User;

public class Main {

	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		
		DataBase db = new DataBase("UserRatings.db");
		
        // SQL statement for creating a new table
		if(!db.exists()) {
			
		    String sql = "CREATE TABLE IF NOT EXISTS userRatings (\n"
		            + "    userid integer NOT NULL,\n"
		            + "    itemid integer NOT NULL,\n"
		            + "    rating double NOT NULL,\n"
		            + "    timestamp integer NOT NULL,\n"
		            + "    unique (userid, itemid, rating, timestamp)\n"
		            + ");";
		
			db.createNewTable(sql);
			
			db.batchInsert("comp3208-train-small.csv");
        
		}
		
		System.out.println("IN PROGRESS: Retrieving items from the database...");
		ArrayList<Item> items = db.contructItems();
		System.out.println("ITEMS SIZE: " + items.size());
		System.out.println("COMPLETE: Retrieving items from the database!");
		
		System.out.println("IN PROGRESS: Retrieving users from the database...");
		Set<User> users = db.constructUsers();
		System.out.println("USERS SIZE: " + users.size());
		System.out.println("COMPLETE: Retrieving users from the database!");
		
		Recommender recommender = new Recommender(users, items);
		
		String similarityFile = "SimilarityMatrix.txt";
		File matrixFile = new File(similarityFile);
		if(!matrixFile.exists()) { //There exists no file of the similarity matrix
			
			System.out.println("IN PROGRESS: Constructing similarity matrix...");
			recommender.autoConstructSimilarityMatrix(); //We have to therefore construct the similarity matrix
			System.out.println("COMPLETE: Constructing similarity matrix!");
			
			System.out.println("IN PROGRESS: Saving similarity matrix...");
			try {
				matrixFile.createNewFile();
	    		ObjectOutputStream outputStream;
				outputStream = new ObjectOutputStream(new FileOutputStream(matrixFile.getAbsolutePath()));
				outputStream.writeObject(recommender.getSimilarityMatrix()); //We go on to save the constructed matrix 
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("COMPLETE: Saving similarity matrix!");
			
		} else {
			
			System.out.println("IN PROGRESS: Loading similarity matrix...");
			ObjectInputStream inputStream;
			try {
				inputStream = new ObjectInputStream(new FileInputStream(matrixFile.getAbsolutePath()));
				double[][] similarityMatrix = (double[][])inputStream.readObject();
				recommender.setSimilarityMatrix(similarityMatrix);
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
			}
			System.out.println("COMPLETE: Loading similarity matrix!");
			
		}
	}

}
