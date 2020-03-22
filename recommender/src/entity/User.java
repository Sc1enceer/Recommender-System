package entity;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class User {
    private int userId;
    private Set<Item> ratedItems = new HashSet<Item>();
    private Set<Score> itemScores = new HashSet<Score>();

    public User(int userId) {
        this.userId = userId;
    }

    public void rate (Item item, double rate){
        this.ratedItems.add(item);
        this.itemScores.add(new Score(item, rate));
    }

    public int getUserId() {
        return userId;
    }

    public Set<Item> getRatedItems() {
        return ratedItems;
    }

    public double getItemScores(Item item) {
        Iterator<Score> iter = itemScores.iterator();
        while (iter.hasNext()){
            Score itemScore = iter.next();
            if (itemScore.getItem().getItemId() == item.getItemId()){
                return itemScore.getScore();
            }
        }
        return 0.0;
    }

    @Override
    public String toString() {
        return "user [userId=" + userId + "]";
    }
}