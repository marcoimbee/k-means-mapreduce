package it.unipi.hadoop.map_reduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.point.Point;

public class KMeansReducer extends Reducer<IntWritable, Point, Text, Text> {

    private Text keyCentroid = new Text();          //OutputKey
    private Text valueCentroid = new Text();        //OutputValue
    
    public void reduce(IntWritable centroid, Iterable<Point> list, Context context) throws IOException, InterruptedException {
        Point total = list.iterator().next().copy();    //Take the first element from the point's list

        while (list.iterator().hasNext()) {             //Scroll the list and sum the value of the points
            total.sum(list.iterator().next());          //The sum will be stored in a specific attribute called AggregatedPoints 
        }
        
        //Calculate the mean
        float[] totalCoords = total.getCoords();        //Read the coords from the point

        for(int i = 0; i < total.getDim(); i++) {       //Divide each coord with the Sum of the coords
            totalCoords[i] = totalCoords[i] / (float)total.getAggregatedPoints() /*+ count */;
        }
            
        total.setCoords(totalCoords);                   //Set the coords of the centroids before the emit process
        total.setAggregatedPoints(1);

        //KeyCentroid and valueCentroid are Text, so for using the set method we have to convert in String
        keyCentroid.set(centroid.toString());
        valueCentroid.set(total.toString());

        //Emit the value
        context.write(keyCentroid, valueCentroid);
    }
}
