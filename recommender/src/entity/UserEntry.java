package entity;

import java.io.Serializable;

public class UserEntry implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private int userid;
	private float rating;
	private int timestamp;
	
	public UserEntry(int userid, float rating, int timestamp) {
		this.userid = userid;
		this.rating = rating;
		this.timestamp = timestamp;
	}
	
	public int getUserID() { //Gets the id of the user who made this entry
		return userid;
	}
	
	public float getRating() { //Gets the rating of this entry
		return rating;
	}
	
	public int getTimeStamp() { //Gets the time stamp for when this entry was created
		return timestamp;
	}
	
}
