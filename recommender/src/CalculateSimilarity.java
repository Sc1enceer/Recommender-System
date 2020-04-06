import entity.Item;
import entity.Pair;
import entity.Score;
import entity.User;
import entity.UserEntry;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CalculateSimilarity {
	
	private HashMap<Integer, Double> averageRatings = null;

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



    private static double sum;

	/*
     * Deprecated - DO NOT USE
     */
    @Deprecated
    public float itemCosineDist2(Item item1, Item item2, Set<User> userSet){
        float dist = 0.0f;
        float numerator = 0.0f;
        float denominator = 0.0f;


        for(User user :  userSet) {
            float score1 = 0.0f;
            float score2 = 0.0f;
            double avgUserRating = avgRating(user);
            if(user.getRatedItems().contains(item1) && user.getRatedItems().contains(item2)){
                score1 = (float) (user.getItemScores(item1) - avgUserRating);
                score2 = (float) (user.getItemScores(item2) - avgUserRating);
                numerator += score1 * score2;
                denominator += Math.sqrt(Math.pow(score1, 2)) * Math.sqrt(Math.pow(score2, 2));
                dist += numerator/denominator;
            }
        }
        return dist;
    }

    @Deprecated
    private static float avgRating(User user){
        Set<Item> items = user.getRatedItems();
        int counter = 1;
        float total_rating = 0.0f;
        for(Item item : items){
            total_rating += (float)user.getItemScores(item);
            counter++;
        }
        return total_rating/counter;
    }
    
    public void calculateAverageRatings(Collection<Set<UserEntry>> data) {
		
    	HashMap<Integer, Accumulator> acc = new HashMap<>();
    	
    	Iterator<Set<UserEntry>> iterator = data.iterator();
    	while(iterator.hasNext()) {
    		Set<UserEntry> entry = iterator.next();
    		Iterator<UserEntry> userIterator = entry.iterator();
    		while(userIterator.hasNext()) {
    			UserEntry user = userIterator.next();
    			if(acc.containsKey(user.getUserID())) {
    				acc.put(user.getUserID(), acc.get(user.getUserID()).addtoAccumulator(user.getRating()));
    			} else {
    				acc.put(user.getUserID(), new Accumulator(user.getRating()));
    			}
    		}
    	}
    	
    	HashMap<Integer, Double> averages = new HashMap<>();
    	for(Entry<Integer, Accumulator> entry : acc.entrySet()) {
    		averages.put(entry.getKey(), entry.getValue().getAverage());
    	}
    	
    	this.averageRatings = averages;
    }
    
    class Accumulator {
    	
   		private double sum;
   		private int total;
   		
   		Accumulator(double sum) {
			this.sum = sum;
			this.total = 1;
		}
   		
   		public Accumulator addtoAccumulator(double addTo) {
   			sum += addTo;
   			total++;
   			return this;
   		}
   		
   		public double getAverage() {
   			return sum / total;
   		}
	}
    
    public Double itemCosineDist(Set<Pair> entries) { //Will find the cosine distance between two items
    	
    	if(entries.isEmpty())
    		return null;
    	
    	double sumTop = 0.0;
    	double sumBotL = 0.0;
    	double sumBotR = 0.0;
    	
    	Iterator<Pair> iterator = entries.iterator();
    	while(iterator.hasNext()) {
    		Pair pair = iterator.next();
    		sumTop += ((pair.getFirst().getRating() - averageRatings.get(pair.getFirst().getUserID()))*(pair.getSecond().getRating() - averageRatings.get(pair.getSecond().getUserID())));
    		sumBotL += Math.pow((pair.getFirst().getRating() - averageRatings.get(pair.getFirst().getUserID())), 2);
    		sumBotR += Math.pow((pair.getSecond().getRating() - averageRatings.get(pair.getSecond().getUserID())), 2);
    	}
    	
    	if(sumTop == 0)
    		return 0.0;
    	if((Math.sqrt(sumBotL) * Math.sqrt(sumBotR)) == 0)
    		return 1.0; //Not sure if we should return 1 if the denom is 0

		return (sumTop / (Math.sqrt(sumBotL) * Math.sqrt(sumBotR)));
    }

}