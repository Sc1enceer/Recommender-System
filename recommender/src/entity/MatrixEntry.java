package entity;

public class MatrixEntry {
	
	private Integer item_a;
	private Integer item_b;
	private Double score;
	
	public MatrixEntry(Integer item_a, Integer item_b, Double score) {
		this.item_a = item_a;
		this.item_b = item_b;
		this.score = score;
	}
	
	public Integer getItemA() { //Gets the id of the user who made this entry
		return item_a;
	}
	
	public Integer getItemB() { //Gets the rating of this entry
		return item_b;
	}
	
	public Double getScore() { //Gets the time stamp for when this entry was created
		return score;
	}
	
}
