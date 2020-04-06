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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import entity.Item;
import entity.User;
import entity.UserEntry;

public class Main {

	@SuppressWarnings({ "static-access", "unchecked" })
	public static void main(String[] args) {
		
		DataBase db = new DataBase("UserRatings.db");
		
		String defaultTable = "userRatings";
		
        // SQL statement for creating a new table
		if(!db.exists() && db.tableExists(defaultTable)) {
			
		    String sql = "CREATE TABLE IF NOT EXISTS " + defaultTable + " (\n"
		            + "    userid integer NOT NULL,\n"
		            + "    itemid integer NOT NULL,\n"
		            + "    rating double NOT NULL,\n"
		            + "    timestamp integer NOT NULL,\n"
		            + "    unique (userid, itemid, rating, timestamp)\n"
		            + ");";
		
			db.createNewTable(sql);
			
			db.batchInsert("comp3208-train-small.csv");
        
		}
		
	    /*
	     DECRECATED METHOD - DO NOT USE
	    
		System.out.println("IN PROGRESS: Retrieving items from the database...");
		ArrayList<Item> items = db.contructItems();
		System.out.println("ITEMS SIZE: " + items.size());
		System.out.println("COMPLETE: Retrieved items from the database!");
		
		System.out.println("IN PROGRESS: Retrieving users from the database...");
		Set<User> users = db.constructUsers();
		System.out.println("USERS SIZE: " + users.size());
		System.out.println("COMPLETE: Retrieved users from the database!");
		 */
		
		String itemHashFile = "Entries.txt";
		File entriesFile = new File(itemHashFile);
		
		HashMap<Integer, Set<UserEntry>> entries = null;
		if(!entriesFile.exists()) {
			
			System.out.println("IN PROGRESS: Retrieving items and users from the database...");
			entries = db.contructItemsHash();
			System.out.println("COMPLETE: Retrieved items and users from the database!");
			
			System.out.println("IN PROGRESS: Saving the entries data structure to a file...");
			try {
				entriesFile.createNewFile();
	    		ObjectOutputStream outputStream;
				outputStream = new ObjectOutputStream(new FileOutputStream(entriesFile.getAbsolutePath()));
				outputStream.writeObject(entries); //We go on to save the constructed matrix 
				outputStream.close();
				
				System.out.println("COMPLETE: Saved the entries data structure to a file!");
			} catch (IOException e) {
				System.out.println("ERROR: Saving the entries data structure to a file.");
				e.printStackTrace();
			}
		} else {
			System.out.println("IN PROGRESS: Loading the entries data structure...");
			ObjectInputStream inputStream;
			try {
				inputStream = new ObjectInputStream(new FileInputStream(entriesFile.getAbsolutePath()));
				entries = (HashMap<Integer, Set<UserEntry>>) inputStream.readObject();	
				System.out.println("COMPLETE: Loaded the entries data structure!");
			} catch (IOException | ClassNotFoundException e) {
				System.out.println("ERROR: Loading the entries data structure.");
			}
		}
		
		if(entries != null) {
			
			Recommender recommender = new Recommender(entries);
			
			String databaseName = "SimilarityMatrix.db";
			File dbFile = new File(databaseName);
			if(!dbFile.exists()) {
				recommender.autoConstructSimilarityDataBase(databaseName);
			} else {
				System.out.println("IN PROGRESS: Setting similarity database...");
				DataBase similarityDataBase = new DataBase(databaseName);
				recommender.setSimilarityDataBase(similarityDataBase);
				System.out.println("COMPLETE: Set similarity database!");
			}
			
			System.out.println("Sim score: " + recommender.getSimilarityScore(113, 1876));
		
		} else {
			System.out.println("ERROR: Loading/Constructing the entries data structure");
		}
	}

}
