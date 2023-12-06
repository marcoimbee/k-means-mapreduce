package it.unipi.hadoop.map_reduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.point.Point;


public class KMeansCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
    
    public void reduce(IntWritable centroid, Iterable<Point> list, Context context) throws IOException, InterruptedException {
    
        // first point of the iterable<Point>
        Point partialTotalPoint = list.iterator().next().copy();

        // Making the partial sums 
        while (list.iterator().hasNext()) {
            partialTotalPoint.sum(list.iterator().next());
        }

        // passing data to the reducer
        context.write(centroid, partialTotalPoint);
    }
}
