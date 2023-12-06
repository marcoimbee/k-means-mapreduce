package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.map_reduce.KMeansCombiner;
import it.unipi.hadoop.map_reduce.KMeansMapper;
import it.unipi.hadoop.map_reduce.KMeansReducer;
import it.unipi.hadoop.point.Point;



public class KMeans {

    /*
     * Extracting the initial centroids that will be used by the K-means algorithm
     */
    private static Point[] startCentroids(Configuration conf, String inputFilePath, int nClusters, int dataSetLength) throws IOException {

        List<Integer> indexes = new ArrayList<Integer>();   //Generate the list of the centroids indexes
        Random rand = new Random();                         

        int extracted = 0;
        int index;

        while(extracted < nClusters){
            index = rand.nextInt(dataSetLength);            
            
            if(indexes.contains(index) == false){           //Extract and index between 0 and datasetlength. This index, will identify the line of the dataset
                indexes.add(index);                         //that will be used to gather the coordinates of the initial centroids
                extracted++;
            }
        }

        indexes.sort(Comparator.naturalOrder());    //Sorting the list in ascendent order

        // Opening and initializing the file system to read the input file
        Path path = new Path(inputFilePath);
    	FileSystem hdfs = FileSystem.get(conf);
    	FSDataInputStream in = hdfs.open(path);         //opening the input file through the hdfs
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        Point[] centroids = new Point[nClusters];          //Creating an Array of centroids

        int count = 0;

        for(int i = 0; i < dataSetLength; i++) {

            String centroid = br.readLine();
            
            if(i == indexes.get(count)) {                   //If the line of the dataset is equal to the random line extracted before, we take the coords 
                String[] splitted = centroid.split(",");
                centroids[count] = new Point(splitted);     //Assign the coords to the centroids
                count++;                                    
            }
        
            if(count == indexes.size())       
                break;          //all the initial centroids have been extracted from the file, we can stop
        }

        br.close();

        return centroids;
    } 

    /*
     * Reading the updated centroids from the files outputted by the reducers in the HDFS
     */
    private static Point[] getNewCentroids(Configuration conf, int nClusters, String reducerOutputPath) throws IOException, FileNotFoundException {
        Point[] centroids = new Point[nClusters];           //array to contain the centroids that we are going to read
        
        FileSystem hdfs = FileSystem.get(conf);

        FileStatus[] status = hdfs.listStatus(new Path(reducerOutputPath));         //getting all the informations of the files in reducerOutputPath

        for (int i = 0; i < status.length; i++) {           //for each file in the output folder
            if(status[i].getPath().toString().endsWith("_SUCCESS") == false) {      //get the path of the i-th file, check if it is the _SUCCESS file
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));          
                String line = br.readLine();                    //reading each reducer's output file, just one line (the centroid's data)
                String[] keyValue = line.split("\t");           //the format is key - tab - value
                String[] components = keyValue[1].split(",");   //getting the centroid's components as an array

                int index = Integer.parseInt(keyValue[0]);      //getting the centroid's index
                
                centroids[index] = new Point(components);       //initializing the centroids' array with the centroids' read data
                
                br.close();
            }
        }

        hdfs.delete(new Path(reducerOutputPath), true);     //deleting the temporary directory for the new iteration

        return centroids;
    }


    /*
     * Checking if the coordinates of the points in currentCentroids differ from the ones of the 
     * points in previousCentroids  by a 'threashold' amount to stop or continue the algorithm
     */
    private static boolean checkThreshold(Point[] currentCentroids, Point[] previousCentroids, float threshold) {

        float dist = 0;
        for(int i = 0; i < currentCentroids.length; i++){
            dist = currentCentroids[i].distance(previousCentroids[i]);          //compunting the distance between the two centroids
            if(dist > threshold)
                return false;           //the centroids differ more than threshold from each other, we can return
        }

        return true;    //all the new centroids are equal to the old ones +/- threshold, we can stop
    }


    /*
     * toString() for a point array
     */
    private static void displayPointArray(Point[] pointArr) {
        for(int i = 0; i < pointArr.length; i++)
            System.out.println("\t" + pointArr[i].displayPoint());
    }


    /*
     * DRIVER CODE
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs(); //Getting the arguments passed through CLI

        long startTimer = 0;    //Setting the timers for the evaluation process
        long endTimer = 0;
        long icStartTimer = 0;
        long icEndTimer = 0;

        startTimer = System.currentTimeMillis();   //Starting the timer for count the time of execution

        if(otherArgs.length < 3){       //less arguments received than expected
            System.out.println("Usage: KMeans <input_filename> <clusters> <output_folder>");
            System.out.println("The input filename should be in the format: datasetSize_pointDimensions.txt");
            System.exit(2);
        }


        //adding the configuration file
        configuration.addResource(new Path("configuration_file.xml"));

        // Initializing parameters and reading the configuration file
        final String INPUT_FILE = "/user/hadoop/kmeans_datasets/" + otherArgs[0];
        final int N_CLUSTERS = Integer.parseInt(otherArgs[1]);
        final String OUTPUT_LOCATION = "/user/hadoop/" + otherArgs[2] + "/temp_reducer_output";
        final int ITERATION_LIMIT = configuration.getInt("IterationLimit", 30);
        final float STOPPING_THRESHOLD = configuration.getFloat("Threshold", 0.01f);
        final int DATASET_SIZE = Integer.parseInt(otherArgs[0].split("_")[0]);

        //setting the CLI-received parameter
        configuration.set("NClusters", Integer.toString(N_CLUSTERS));

        System.out.println("[INFO] Configuration file 'configuration_file.xml' read");
        System.out.println("[INFO] Reading data from file '" + INPUT_FILE + "'");
        System.out.println("[INFO] Running KMeans algorithm with parameters:");
        System.out.println("\t-> points: " + DATASET_SIZE);
        System.out.println("\t-> clusters: " + N_CLUSTERS);
        System.out.println("\t-> stopping threshold: " + STOPPING_THRESHOLD + " (or when reached " + ITERATION_LIMIT + " iterations)");

        Point[] currentCentroids = new Point[N_CLUSTERS];
        Point[] previousCentroids = new Point[N_CLUSTERS];
        
        //Getting the initial centroids
        icStartTimer = System.currentTimeMillis();
        currentCentroids = startCentroids(configuration, INPUT_FILE, N_CLUSTERS, DATASET_SIZE);
        icEndTimer = System.currentTimeMillis();

        System.out.println("[INFO] Initial centroids:");
        displayPointArray(currentCentroids);
        
        //Setting the centroids in the configuration so that the mapper can retrieve them
        for(int i = 0; i < N_CLUSTERS; i++) {
            configuration.set("centroid_" + i, currentCentroids[i].toString());
        }

        boolean iterationOutcome = true;
        int iterationCounter = 1;
        boolean stopCondition = false;


        //KMEANS ALGORITHM MAIN CYCLE
        while(iterationCounter <= ITERATION_LIMIT){

            System.out.println("[INFO] Iteration " + iterationCounter + " started");

            Job job = Job.getInstance(configuration, "job_" + iterationCounter);
            
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Point.class);
            FileInputFormat.addInputPath(job, new Path(INPUT_FILE));
            FileInputFormat.setInputPaths(job, FileInputFormat.getInputPaths(job));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT_LOCATION));
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //one reducer for each centroid we want to find
            job.setNumReduceTasks(N_CLUSTERS);
            
            iterationOutcome = job.waitForCompletion(true);

            //the failure of a job makes the algorithm stop
            if(!iterationOutcome) {     
                System.err.println("[ERROR] Iteration " + (iterationCounter + 1) + " failed");
                System.exit(1);
            }

            //Save the old centroids and read the new ones
            for(int i = 0; i < N_CLUSTERS; i++) {
                previousCentroids[i] = currentCentroids[i].copy();
            }     

            //reading the newly computed centroids
            currentCentroids = getNewCentroids(configuration, N_CLUSTERS, OUTPUT_LOCATION);

            System.out.println("[INFO] The centroids have been updated");
            System.out.println("[INFO] Updated centroids:");
            displayPointArray(currentCentroids);
            
            //checking the change in the centroids' coordinates
            stopCondition = checkThreshold(currentCentroids, previousCentroids, STOPPING_THRESHOLD);
        
            System.out.println("[INFO] Iteration " + iterationCounter + " finished");

            iterationCounter++;

            //checking wether we should stop or continue
            if(stopCondition == true || iterationCounter > ITERATION_LIMIT){
                break;
            } else {
                for(int j = 0; j < N_CLUSTERS; j++) {           //update the configuration with the new centroids
                    configuration.unset("centroid_" + j);
                    configuration.set("centroid_" + j, currentCentroids[j].toString());
                }
            }
        }

        System.out.println("[INFO] Stopping criterion satisfied");
        
        System.out.println("Final centroids:");
        displayPointArray(currentCentroids);

        endTimer = System.currentTimeMillis();
        System.out.println("[INFO] Execution time: " + (endTimer - startTimer) + " ms");
        System.out.println("[INFO] Initial Centroid time: " + (icEndTimer - icStartTimer) + " ms");
    }
}
