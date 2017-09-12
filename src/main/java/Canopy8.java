import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;

import java.io.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Canopy8 {

    public static final double t2 = 1.0;
    public static final double t1 = 2.0;


    private final static int sMaxMinFeatureNum = 20;
    private final static int sOffset = 3;

    private final static int TYPE_MAX = 1;
    private final static int TYPE_MIN = 0;

    public static class Map2 extends Mapper<Object, Text, NullWritable, Text> {

        ArrayList<Feature> centerFeatures = new ArrayList<Feature>();
        Normalize normalize = new Normalize();

        @Override
        public void setup(Context context) throws IOException {
            ////////////////////////////// Start: read the features max/min for standardiztion //////////////////////////////////////////////
            Path pt = new Path("canopyBotnet/standardization/part-r-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            ////////////////////// read the max
            while ((line = br.readLine()) != null) {
                normalize.fileString(line);
            }
            br.close();
            ////////////////////////////// End: read the features max/min for standardiztion //////////////////////////////////////////////

            ///////////////////////////////// Start: Read the center point ////////////////////////////////////////////////////////////////
            Path pt2 = new Path("canopyBotnet/Center/part-r-00000");
            FileSystem fs2 = FileSystem.get(new Configuration());
            BufferedReader br2 = new BufferedReader(new InputStreamReader(fs2.open(pt2)));
            String line2;

            while ((line2 = br2.readLine()) != null) {
                Feature temp = new Feature();
                temp.setupString(line2);
                centerFeatures.add(temp);
            }
            br2.close();
            ///////////////////////////////// End: Read the center point ///////////////////////////////////////////////////////////////////
        }//end of map setup

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Feature dataFeature = new Feature(value.toString()); //cache variable

            dataFeature.doNormalize(normalize);

            for (Feature temp : centerFeatures) {
                temp.doNormalize(normalize);
                double distnace = temp.getDistance(dataFeature);
                if (distnace < t1) {
                    dataFeature.addTag(temp);
                }
            }

            context.write(NullWritable.get(), new Text(dataFeature.getString()));
        }//end of map
    }// end of map2


    public static class Reduce2 extends Reducer<NullWritable, Text, NullWritable, Text>
    {

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {

            for(Text tempString : values)
            {
                context.write(NullWritable.get(),new Text(tempString.toString()));
            }
        }
    }

    //////////////////////////////////////// 1st stage mapreduce ////////////////////////////////////////////////////////////
    public static class Map extends Mapper<Object, Text, IntWritable, Text> {
        private Text word = new Text();

        ArrayList<Feature> centerFeatures = new ArrayList<Feature>();
        Normalize normalize = new Normalize();

        @Override
        public void setup(Context context) throws IOException {
            Path pt = new Path("canopyBotnet/standardization/part-r-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;

            while ((line = br.readLine()) != null) {
                normalize.fileString(line);
            }

        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Feature dataFeature = new Feature(value.toString()); //cache variable


            ////////////////////////////////////// if the center is empty then add the first one on it ///////////////////////////////////////
            if (centerFeatures.isEmpty())
            {
                centerFeatures.add(dataFeature);
                context.write(new IntWritable(1), new Text(dataFeature.getString()));
            }
            else
            {

                dataFeature.doNormalize(normalize);

                for (Feature temp : centerFeatures)
                {
                    temp.doNormalize(normalize);
                    double distnace = temp.getDistance(dataFeature);
                    if (distnace < t2)
                    {
                        dataFeature.remove(true);
                        break;
                    }
                }

                if (!dataFeature.getRemove())
                {
                    centerFeatures.add(dataFeature);
                    context.write(new IntWritable(1), new Text(dataFeature.getString()));
                }

            }//end of else
        } //end of map
    }//end of Mapclass

    public static class Reduce extends Reducer<IntWritable, Text, NullWritable, Text> {

        ArrayList<Feature> centerFeatures = new ArrayList<Feature>();
        Normalize normalize = new Normalize();

        @Override
        public void setup(Context context) throws IOException
        {
            Path pt = new Path("canopyBotnet/standardization/part-r-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = br.readLine()) != null)
            {
                normalize.fileString(line);
            }

            br.close();
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
        {
            ////////////////////////////////////// if the center is empty then add the first one on it ///////////////////////////////////////
            for (Text lineString : value)
            {
                Feature dataFeature = new Feature(lineString.toString());
                dataFeature.doNormalize(normalize);
                if (centerFeatures.isEmpty())
                {
                    centerFeatures.add(dataFeature);
                    context.write(NullWritable.get(), new Text(dataFeature.getString()));
                }
                else
                {
                    for (Feature temp : centerFeatures)
                    {
                        temp.doNormalize(normalize);
                        double distnace = temp.getDistance(dataFeature);
                        if (distnace < t2)
                        {
                            dataFeature.remove(true);
                        }
                        if(dataFeature.getRemove())
                            break;
                    }

                    if (!dataFeature.getRemove())
                    {
                        centerFeatures.add(dataFeature);
                        context.write(NullWritable.get(), new Text(dataFeature.getString()));
                    }
                }
            }//end of else
        }//end of reducer
    }//end of reduce

    public static void main(String[] args) throws Exception {
        boolean verbose = false;
        ///////////////////////// job 1 ////////////////////////////////////////////
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Canopy_MapReduce_GetStandardization");
        System.out.println("Job> Canopy_MapReduce_B");

        FileSystem fs1 = FileSystem.get(conf);
        job1.setJarByClass(Canopy8.class);
        job1.setNumReduceTasks(1);

        //job.setMapperClass(TokenizerMapper.class);
        //job.setReducerClass(IntSumReduce.class);
        job1.setMapperClass(Phase0Mapper.class);
        job1.setReducerClass(Phase0Reducer.class);
        //job.setCombinerClass(IntSumReducer.class);
        // job.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        if (fs1.exists(new Path(args[1])))
            fs1.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.out.println("intput path-> " + args[0]);
        System.out.println("output path-> " + args[1]);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        /////////////////// wait job1 done the job 1st ////////////////////////////////
        if (job1.waitForCompletion(true)) {
            System.out.println("Doing second times mapreduce...");
        } else {
            System.out.println("Trouble on fisrt mapreduce");
            System.exit(1);
        }
//////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("==========");
        System.out.println(" Max and Min ");
        System.out.println("==========");

        Path standardization = new Path("canopyBotnet/standardization/part-r-00000");
        FileSystem readFile = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(readFile.open(standardization)));

        Normalize normalize = new Normalize();
        String line;
        while ((line = br.readLine()) != null) {
            normalize.fileString(line);
        }
        System.out.println(normalize.getPrint());

        br.close();
        System.out.println("");
        System.out.println("Done.");
        //
        System.out.println("Doing second times mapreduce...");
///////////////////////////////// job2 ///////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Canopy_MapReduce_Round2");
        System.out.println("Job2> Canopy_MapReduce_C");

        FileSystem fs2 = FileSystem.get(conf2);
        job2.setJarByClass(Canopy8.class);
        job2.setNumReduceTasks(1);


        job2.setMapperClass(Map.class);
        job2.setReducerClass(Reduce.class);
        //job.setCombinerClass(IntSumReducer.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        if (fs2.exists(new Path(args[2])))
            fs2.delete(new Path(args[2]), true);

        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.out.println("intput path-> " + args[0]);
        System.out.println("output path-> " + args[2]);
        //System.exit(job2.waitForCompletion(true) ? 0 : 1);
        /////////////////////////////////////// wait for job 2 done //////////////////////////////////////////////
        if (job2.waitForCompletion(true)) {
            System.out.println("Doing third times mapreduce...");
        } else {
            System.out.println("Trouble on second mapreduce");
            System.exit(1);
        }
        ////////////////////////////// job3 ///////////////////////////////////////////

        /////////////////////////////////////////////////////////////////////////
        System.out.println("==========");
        System.out.println("  test length ");
        System.out.println("==========");

        Path center = new Path("canopyBotnet/Center/part-r-00000");
        FileSystem readFile2 = FileSystem.get(new Configuration());
        BufferedReader br2 = new BufferedReader(new InputStreamReader(readFile2.open(center)));
        line = br2.readLine();
        System.out.println(line);
        String[] ldt = line.split("\t");
        System.out.println(ldt.length);
        br2.close();
/////////////////////////////////////////////////////////////////////////////////
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Canopy_MapReduce iteration");
        System.out.println("Job2> Canopy_MapReduce_D");

        FileSystem fs3 = FileSystem.get(conf3);
        job3.setJarByClass(Canopy8.class);
        job3.setNumReduceTasks(1);

        job3.setMapperClass(Map2.class);
        job3.setReducerClass(Reduce2.class);

        job3.setMapOutputKeyClass(NullWritable.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);

        if (fs3.exists(new Path(args[3])))
            fs3.delete(new Path(args[3]), true);

        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        System.out.println("input path-> " + args[0]);
        System.out.println("output path0> " + args[3]);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);

    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 0 
    public static class Phase0Mapper extends Mapper<LongWritable, Text, Text, Text>
    {
        // counter
        private int mFeatureInit = 0;

        // feature vector max, min value
        private double[] mFVMax = new double[sMaxMinFeatureNum];
        private double[] mFVMin = new double[sMaxMinFeatureNum];

        // map intermediate value
        private Text interKey = new Text();
        private Text interValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
      /*
      Input format : 
      Key : offset (LongWritable)
      Value :  
      0:[TIMESTAMP]        1:[PROTOCOL]            2:[FLOW_ID(SrcIP:SrcPort>DstIP:DstPort)] 
      3:[FORWARD_PKTS]     4:[FORWARD_BYTES]       5:[FORWARD_MAX_BYTES]     6:[FORWARD_MIN_BYTES]     7:[FORWARD_MEAN_BYTES]           
      8:[BACKWARD_PKTS]    9:[BACKWARD_BYTES]     10:[BACKWARD_MAX_BYTES]   11:[BACKWARD_MIN_BYTES]   12:[BACKWARD_MEAN_BYTES]            
     13:[FLOW_PKTS]       14:[FLOW_BYTES]         15:[FLOW_MAX_BYTES]       16:[FLOW_MIN_BYTES]       17:[FLOW_MEAN_BYTES]       
     18:[FLOW_STD_BYTES]  19:[FLOW_PKT_PER_MS]    20:[FLOW_BYTES_PER_MS]    21:[FLOW_BYTE_IO_RATIO]   22:[FLOW_DURATION]    
     23:[FLOW_LOSS]       24:[FLOW_SERIAL_NUM]
      */

            //String[] aFeatures = null;
            ArrayList<String> aFeatures = new ArrayList<String>();
            try
            {
                //aFeatures = value.toString().split("\t") ;
                StringTokenizer stk = new StringTokenizer(value.toString());
                while (stk.hasMoreTokens())
                {
                    aFeatures.add(stk.nextToken());
                }
            }
            catch (Exception e)
            {
                return;
            }

            for (int i = 0; i < sMaxMinFeatureNum; i++)
            {
                try
                {
          /*double aValue = Double.parseDouble( aFeatures [i + sOffset] );*/
                    double aValue = Double.parseDouble(aFeatures.get(i + sOffset));
                    if (mFeatureInit == 0)
                    {
                        mFVMax[i] = aValue;
                        mFVMin[i] = aValue;
                    }
                    else
                    {
                        if (aValue > mFVMax[i])
                        {
                            mFVMax[i] = aValue;
                        }
                        if (aValue < mFVMin[i])
                        {
                            mFVMin[i] = aValue;
                        }
                    }
                }
                catch (Exception e)
                {
                    //don't care continue, parseDouble may occur exception if the feature is not an number.
                }
            }
            mFeatureInit++;
        }

        // do the cleanup at the end of the map job
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            StringBuilder maxBuilder = new StringBuilder();
            StringBuilder minBuilder = new StringBuilder();
            for (int i = 0; i < mFVMax.length; i++) {
                maxBuilder.append(mFVMax[i]).append("\t");
            }
            for (int i = 0; i < mFVMin.length; i++) {
                minBuilder.append(mFVMin[i]).append("\t");
            }

            //Keep codes as clean as possible.
            interKey.set("max");
            interValue.set(maxBuilder.toString());
            context.write(interKey, interValue);

            interKey.set("min");
            interValue.set(minBuilder.toString());
            context.write(interKey, interValue);
        }

    }

    public static class Phase0Reducer extends Reducer<Text, Text, Text, Text>
    {
        private int mFeatureMaxInit = 0;
        private int mFeatureMinInit = 0;

        private double[] mFVMax = new double[sMaxMinFeatureNum];
        private double[] mFVMin = new double[sMaxMinFeatureNum];

        // reduce output value
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
      /*
      Input Format
      Key : { max | min }
      value :                                                         
      0:[FORWARD_PKTS]     1:[FORWARD_BYTES]       2:[FORWARD_MAX_BYTES]     3:[FORWARD_MIN_BYTES]     4:[FORWARD_MEAN_BYTES]           
      5:[BACKWARD_PKTS]    6:[BACKWARD_BYTES]      7:[BACKWARD_MAX_BYTES]    8:[BACKWARD_MIN_BYTES]    9:[BACKWARD_MEAN_BYTES]            
     10:[FLOW_PKTS]       11:[FLOW_BYTES]         12:[FLOW_MAX_BYTES]       13:[FLOW_MIN_BYTES]       14:[FLOW_MEAN_BYTES]       
     15:[FLOW_STD_BYTES]  16:[FLOW_PKT_PER_MS]    17:[FLOW_BYTES_PER_MS]    18:[FLOW_BYTE_IO_RATIO]   19:[FLOW_DURATION]    
      */


            for (Text val : values)
            {
                try {
                    String[] aFeatures = val.toString().split("\t");
                    int type = 0;
                    // decide the max features or min features
                    if (key.toString().equals("max")) {
                        type = TYPE_MAX;
                    } else if (key.toString().equals("min")) {
                        type = TYPE_MIN;
                    }

                    for (int i = 0; i < aFeatures.length; i++) {
                        double aValue = Double.parseDouble(aFeatures[i]);
                        if (type == TYPE_MAX) {
                            if (mFeatureMaxInit == 0) {
                                mFVMax[i] = aValue;
                            } else {
                                if (mFVMax[i] < aValue) {
                                    mFVMax[i] = aValue;
                                }
                            }
                        } else if (type == TYPE_MIN) {
                            if (mFeatureMinInit == 0) {
                                mFVMin[i] = aValue;
                            } else {
                                if (mFVMin[i] > aValue) {
                                    mFVMin[i] = aValue;
                                }
                            }
                        }
                    }// end of inner for-loop
                    if (type == TYPE_MAX) {
                        mFeatureMaxInit++;
                    } else if (type == TYPE_MIN) {
                        mFeatureMinInit++;
                    }
                } catch (Exception e) {

                } // end of catch
            } //end of for
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            StringBuilder maxBuilder = new StringBuilder();
            StringBuilder minBuilder = new StringBuilder();
            for (int i = 0; i < mFVMax.length; i++) {
                maxBuilder.append(mFVMax[i]).append("\t");
            }
            for (int i = 0; i < mFVMin.length; i++) {
                minBuilder.append(mFVMin[i]).append("\t");
            }

            //IF code is stable, please remember to remove those unpleasure debug messaages.
            outputKey.set("max");
            outputValue.set(maxBuilder.toString());
            context.write(outputKey, outputValue);

            outputKey.set("min");
            outputValue.set(minBuilder.toString());
            context.write(outputKey, outputValue);
        }
    }
}