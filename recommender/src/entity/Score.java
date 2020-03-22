package entity;

public class Score {
    private Item item;
    private double score;

    public Score(Item item, double score) {
        this.item = item;
        this.score = score;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}