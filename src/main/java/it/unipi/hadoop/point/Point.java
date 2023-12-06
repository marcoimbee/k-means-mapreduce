package it.unipi.hadoop.point;

import java.io.*;
import org.apache.hadoop.io.Writable;


public class Point implements Writable, Comparable<Point>{
    
    private float[] coords;         //point coordinates
    private int dim;                //point dimensions
     
    //if equal to 1 --> single point, if > 1: the point is actually a partial sum of aggregatedPoints points belonging to the same clusters.
    //This is important for remember how many points are semi-summed by the combiner for make the mean
    private int aggregatedPoints;        


    public Point(){
        this.dim = 0;
        this.coords = null;
        this.aggregatedPoints = 1;
    }


    //Return the coords from the object considerated 
    public float[] getCoords() {
        return this.coords;
    }

    //Setting the coords of the point, passed by an array
    public void setCoords(float[] coords) {

        //If the dimension is different an error will be generated
        if(coords.length != this.dim) {
            System.out.println("Error: the two points have different dimensions");
            return;
        }
        for(int i = 0; i < coords.length; i++) {
            this.coords[i] = coords[i];
        }
    }

    //A method for return the dimension
    public int getDim() {
        return this.dim;
    }

    //A method for return the number of the points semi-summed
    public int getAggregatedPoints() {
        return this.aggregatedPoints;
    }

    //A method for setting the attribute aggregated points
    public void setAggregatedPoints(int aggregatedPoints) {
        this.aggregatedPoints = aggregatedPoints;
    }

    //A constructor of the class point that create a new point from an array of string that identify the coords.
    //In this case the array of string is meaning a point
    public Point(String[] file_coords){     //coord1 coord2 coord3 ...
        this.dim = file_coords.length;
        this.coords = new float[this.dim];
        this.aggregatedPoints = 1;
        for(int i = 0; i < dim; i++){
            this.coords[i] = Float.parseFloat(file_coords[i]);
        }
    }

    //Same constructor as above only dealing with arrays of floats identifying a point
    public Point(float[] coords){     //coord1 coord2 coord3 ...
        this.dim = coords.length;
        this.aggregatedPoints = 1;
        this.coords = new float[this.dim];
        for(int i = 0; i < dim; i++){
            this.coords[i] = coords[i];
        }
    }

    //A method for display a point in the console
    public String displayPoint(){
        String pointToText = "(";
        for(int i = 0; i < this.dim - 1; i++){
            pointToText += this.coords[i] + ", ";
        }
        pointToText += this.coords[this.dim - 1];
        pointToText += ")";

        return pointToText;
    }

    //de-serialization where DataInput in is a byte stream that will be converted in a point
    @Override
    public void readFields(DataInput in) throws IOException{
        dim = in.readInt();
        aggregatedPoints = in.readInt();
        
        coords = new float[dim];

        for(int i = 0; i < dim; i++) {
            coords[i] = in.readFloat();
        }
    }

    //serialization where DataOutput is a byte stream, and convert point into a byte stream
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(dim);
        out.writeInt(aggregatedPoints);

        for(int i = 0; i < dim; i++) {
            out.writeFloat(coords[i]);
        }
    }

    //A method that copy a point into another
    public Point copy(){
        Point newPoint = new Point(this.coords);
        newPoint.aggregatedPoints = this.aggregatedPoints;
        return newPoint;
    }

    //A method for convert a float point into a string point
    @Override
    public String toString() {
        String point = "";
        for(int i = 0; i < this.dim; i++) {
            if(i <= this.dim - 2)
                point += Float.toString(this.coords[i]) + ",";
            else 
                point += Float.toString(this.coords[i]);
        }
        return point;
    }


    //A method that calculate the euclidean distance from two point
    public float distance(Point p){
        float sum = 0;

        for(int i = 0; i < this.dim; i++){
            sum += Math.pow(this.coords[i] - p.coords[i], 2);
        }

        return (float)Math.sqrt(sum);
    }

    //A method that calculate the sum from two points, the aggregatedPoints attribute is very important for knowing how many points are summed
    //In the combiner part
    public void sum(Point p) {
        this.aggregatedPoints += p.aggregatedPoints;
        for (int i = 0; i < this.dim; i++) {
            this.coords[i] += p.coords[i];
        }
    }

    @Override
    public int compareTo(Point o) {
        throw new UnsupportedOperationException("Unimplemented method 'compareTo'");
    }
}
