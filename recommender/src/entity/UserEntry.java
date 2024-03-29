package entity;

public class UserEntry {
	
	private int userid;
	private double rating;
	private int timestamp;
	
	public UserEntry(int userid, double rating, int timestamp) {
		this.userid = userid;
		this.rating = rating;
		this.timestamp = timestamp;
	}
	
	public int getUserID() { //Gets the id of the user who made this entry
		return userid;
	}
	
	public double getRating() { //Gets the rating of this entry
		return rating;
	}
	
	public int getTimeStamp() { //Gets the time stamp for when this entry was created
		return timestamp;
	}
	
}
