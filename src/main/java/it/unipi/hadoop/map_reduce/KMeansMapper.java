package it.unipi.hadoop.map_reduce;

//Mapper imports
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import it.unipi.hadoop.point.Point;
import org.apache.hadoop.conf.Configuration;


//Mapper class
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

    private Point[] centroids;
    private Point point;
    private final IntWritable closestCentroid = new IntWritable();         //emitted centroid index -> the one closest to the checked point

    public void setup(Context context){
        
        Configuration conf = context.getConfiguration();
        int n_clusters = conf.getInt("NClusters", 0);       //0 by default, k when reading

        this.centroids = new Point[n_clusters];        //Creating the centroids (they are points in a number equal to the clusters)

        for(int i = 0; i < n_clusters; i++){
            String[] centroid_i = conf.getStrings("centroid_" + i);
            this.centroids[i] = new Point(centroid_i);
        }
    }

    //Implementation of the map function
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        //point gets read from file and passed to map function as Text. We compose an array of its coordinates by converting the Text and splitting it
        String[] pointCoordsString = value.toString().split(",");   

        //constructing the point that is going to be checked
        point = new Point(pointCoordsString);

        //compute the distance from point to each centroid
        float minDistance = Float.POSITIVE_INFINITY;
        float currentDistance = 0;
        int closestCentroidIndex = -1;
        
        for(int i = 0; i < centroids.length; i++){
            currentDistance = point.distance(centroids[i]);

            if(currentDistance < minDistance){
                minDistance = currentDistance;
                closestCentroidIndex = i;
            }
        }

        closestCentroid.set(closestCentroidIndex);      //setting serilizable output key
        context.write(closestCentroid, point);          //emitting the key-value pair
    }

}
