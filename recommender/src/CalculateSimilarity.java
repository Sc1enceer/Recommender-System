import entity.Item;
import entity.Score;
import entity.User;

import java.util.Map;
import java.util.Set;

public class CalculateSimilarity {

    public static double userEuclidDist(User user1, User user2,
                                    Set<Item> itemSet){
        double sum = 0;
        for(Item item: itemSet){
            double score1 = 0.0;
            double score2 = 0.0;
            int itemId = item.getItemId();
            if(user1.getRatedItems().contains(item) && user2.getRatedItems().contains(item)){
                score1 = user1.getItemScores(item);
                score2 = user2.getItemScores(item);
            } else if (user1.getRatedItems().contains(item)){
                score1 = user1.getItemScores(item);
            } else if (user2.getRatedItems().contains(item)){
                score2 = user2.getItemScores(item);
            }

            double temp = Math.pow((score1 - score2), 2);
            sum += temp;
        }
        return Math.sqrt(sum);
    }


    public static double userCosineDist(User user1, User user2,
                                    Set<Item> itemSet){
        double dist = 0;
        double numerator = 0;
        double denominator1 = 0;
        double denominator2 = 0;

        for(Item item: itemSet){
            double score1 = 0.0;
            double score2 = 0.0;
            if(user1.getRatedItems().contains(item) && user2.getRatedItems().contains(item)){
                score1 = user1.getItemScores(item);
                score2 = user2.getItemScores(item);
            } else if (user1.getRatedItems().contains(item)){
                score1 = user1.getItemScores(item);
            } else if (user2.getRatedItems().contains(item)){
                score2 = user2.getItemScores(item);
            }
            denominator1 += Math.pow(score1, 2);
            denominator2 += Math.pow(score2, 2);
        }

        dist = ((1.0 * numerator) / (Math.sqrt(denominator1) * Math
                .sqrt(denominator2)));
        return dist;
    }



    /*
     * userSet: set of user who have rated both i1 and i2
     */
    public double itemCosineDist(Item item1, Item item2, Set<User> userSet){
        double dist = 0;
        double numerator = 0;
        double denominator = 0;


        for(User user :  userSet){
            double score1 = 0.0;
            double score2 = 0.0;
            double avgUserRating = avgRating(user);
            if(user.getRatedItems().contains(item1) && user.getRatedItems().contains(item2)){
                score1 = user.getItemScores(item1) - avgUserRating;
                score2 = user.getItemScores(item2) - avgUserRating;
                numerator += score1 * score2;
                denominator += Math.sqrt(Math.pow(score1, 2)) * Math.sqrt(Math.pow(score2, 2));
                dist += numerator/denominator;
            }
        }
        return dist;
    }

    private static double avgRating(User user){
        Set<Item> items = user.getRatedItems();
        int counter = 1;
        double total_rating = 0.0;
        for(Item item : items){
            total_rating += user.getItemScores(item);
            counter ++;
        }
        return total_rating/counter;
    }


}