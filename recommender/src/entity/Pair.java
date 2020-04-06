package entity;

//Used for simpler calculations of cosine dist, first and second entry would be from the same user for two separate items 
public class Pair {
	private UserEntry first;
	private UserEntry second;
	
	public Pair(UserEntry first, UserEntry second) {
		this.first = first;
		this.second = second;
	}
	
	public UserEntry getFirst() {
		return first;
	}
	
	public UserEntry getSecond() {
		return second;
	}
}