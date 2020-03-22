import entity.Item;
import entity.User;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Recommender {

    private Set<User> users;
    private ArrayList<Item> items;
    double[][] similarityMatrix;
    int numItems;

    public Recommender(Set<User> users, ArrayList<Item> items) {
        this.users = users;
        this.items = items;
        this.numItems = items.size();
    }

    public void autoConstructSimilarityMatrix() {
    	this.similarityMatrix = constructSimilarityMatrix(items);
    }
    
    public void setSimilarityMatrix(double[][] matrix) {
    	this.similarityMatrix = matrix;
    }
    // constructing the similarity matrix

    public double[][] constructSimilarityMatrix(ArrayList<Item> items){
        double[][] matrix = new double[numItems][numItems];
        CalculateSimilarity calculator = new CalculateSimilarity();
        for(int i = 0; i<= items.size(); i++){
            for(int j = 0; j <= items.size(); j++){
                Set<User> commonUser = findUsers(users, items.get(i), items.get(j));
                double similarityScore = calculator.itemCosineDist(items.get(i), items.get(j), commonUser);
                matrix[i][j] = similarityScore;
            }
            System.out.println((((i-1)*items.size())) + "/" + items.size()*items.size() + " items complete! (" + (((i-1)*items.size())/items.size()*items.size()) + "%");
        }
        return matrix;
    }

    private Set<User> findUsers(Set<User> users, Item item1, Item item2){
        Set<User> commonUser = new HashSet<>();
        for(User user : users){
            if (user.getRatedItems().contains(item1) && user.getRatedItems().contains(item2)){
                commonUser.add(user);
            }
        }
        return commonUser;
    }

    private double predictRating(User user, Item item){
        double nominator = 0.0;
        double denominator = 0.0;

        int index = items.indexOf(item);
        for(int i = 0; i < numItems; i++){
            if(i != index){
                nominator += similarityMatrix[i][index] * user.getItemScores(items.get(i));
                denominator += similarityMatrix[i][index];
            }
        }
        return nominator/denominator;
    }
    
    public double[][] getSimilarityMatrix() {
    	return similarityMatrix;
    }

}