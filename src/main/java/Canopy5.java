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

public class Canopy5
{
  //static String outputPath = "";
  //static String out_temp = "points_temp";
  public static String IN = "";
  public static final double t2 = 1.0;
  public static final double t1 = 2.0;

  private final static int sMaxTokenNumPhase1 = 25;
  private final static int sMaxTokenNumPhase2 = 26;
  private final static int sMaxMinFeatureNum = 20 ;
  private final static int sOffset = 3 ;
  private final static int sOffset2 = 2 ;
  private final static int sP2ReduceOff = 1 ;
  private final static int sMaxMinOffset = 1 ;
  private final static int TYPE_MAX = 1;
  private final static int TYPE_MIN = 0;  


  public static class Map2 extends Mapper<Object,Text,NullWritable,Text>
   {
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();

      ArrayList<String> dataCenterNetFlow = new ArrayList<String>();

      ArrayList<String> featureMax = new ArrayList<String>();
      ArrayList<String> featureMin = new ArrayList<String>();

    @Override
    public void setup(Context context)throws IOException
    {
      ////////////////////////////// Start: read the features max/min for standardiztion //////////////////////////////////////////////
      Path pt = new Path("canopyBotnet/standardization/part-r-00000");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;
      ////////////////////// read the max 
      line = br.readLine();
      StringTokenizer stk = new StringTokenizer(line);
      String temp0 = stk.nextToken();
 
      while(stk.hasMoreTokens())
      {
        featureMax.add(stk.nextToken());
      }
      /////////////////////// red the min
      line = br.readLine();
      stk = new StringTokenizer(line);
      temp0 = stk.nextToken();

      while(stk.hasMoreTokens())
      {
        featureMin.add(stk.nextToken());
      }
      br.close();
    ////////////////////////////// End: read the features max/min for standardiztion //////////////////////////////////////////////

    ///////////////////////////////// Start: Read the center point ////////////////////////////////////////////////////////////////
      Path pt2 = new Path("canopyBotnet/Center/part-r-00000");
      FileSystem fs2 = FileSystem.get(new Configuration());
      BufferedReader br2 = new BufferedReader(new InputStreamReader(fs2.open(pt2)));
      String line2;

      while((line2 = br2.readLine())!=null)
      {
        StringTokenizer stk2 = new StringTokenizer(line2);
        String fake = stk2.nextToken();
        while(stk2.hasMoreTokens())
        {
          dataCenterNetFlow.add(stk2.nextToken());
        }
      }
      br2.close(); 
    ///////////////////////////////// End: Read the center point /////////////////////////////////////////////////////////////////// 
    }//end of map setup   
/*    @Override
    public void cleanup(Context context) throws IOException,InterruptedException
    {
      int count = 0;
      while(count<dataCenterNetFlow.size())
      {
        String outStrings = "";
        for(int centerCanopy = 0;centerCanopy<=25;centerCanopy++)
        {
          if(centerCanopy+count<dataCenterNetFlow.size())
          {
               outStrings = outStrings+dataCenterNetFlow.get(centerCanopy+count)+"\t";
          }
          else
          {
              outStrings = outStrings+"\t"+ "error" ;
          }
        }
        if(outStrings!=null)
        {
          context.write(NullWritable.get(),new Text(outStrings));
        }
        else
        {
          String error = "EAFAFCSDVDSVDFVBDFBDFBDFBEBEB";
          context.write(NullWritable.get(),new Text(error));
        }
        count = count + 26;
      }
    }*/

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

     ArrayList<String> dataNetFlow = new ArrayList<String>(); //cache variable

    
     StringTokenizer stk = new StringTokenizer(value.toString());//prase the string from map
     int countToken = 0;//check flag
     String tag = "";
     while(stk.hasMoreTokens())
     {
         dataNetFlow.add(stk.nextToken()); // Add 24 features to the cache variable
     }
      
      double in_forNumOfPkt =  normalize(Double.parseDouble(dataNetFlow.get(3)),Double.parseDouble(featureMax.get(0)),Double.parseDouble(featureMin.get(0)));
      double in_forNumOfByte =  normalize(Double.parseDouble(dataNetFlow.get(4)),Double.parseDouble(featureMax.get(1)),Double.parseDouble(featureMin.get(1)));
      double in_for_max = normalize(Double.parseDouble(dataNetFlow.get(5)),Double.parseDouble(featureMax.get(2)),Double.parseDouble(featureMin.get(2)));
      double in_for_min = normalize(Double.parseDouble(dataNetFlow.get(6)),Double.parseDouble(featureMax.get(3)),Double.parseDouble(featureMin.get(3)));
      double in_for_mean = normalize(Double.parseDouble(dataNetFlow.get(7)),Double.parseDouble(featureMax.get(4)),Double.parseDouble(featureMin.get(4)));
      double in_bacNumOfPkt = normalize(Double.parseDouble(dataNetFlow.get(9)),Double.parseDouble(featureMax.get(6)),Double.parseDouble(featureMin.get(6)));
      double in_bac_max = normalize(Double.parseDouble(dataNetFlow.get(10)),Double.parseDouble(featureMax.get(7)),Double.parseDouble(featureMin.get(7)));
      double in_bac_min =  normalize(Double.parseDouble(dataNetFlow.get(11)),Double.parseDouble(featureMax.get(8)),Double.parseDouble(featureMin.get(8)));
      double in_bac_mean = normalize(Double.parseDouble(dataNetFlow.get(12)),Double.parseDouble(featureMax.get(9)),Double.parseDouble(featureMin.get(9)));
      double in_flowNumOfByte = normalize(Double.parseDouble(dataNetFlow.get(14)),Double.parseDouble(featureMax.get(11)),Double.parseDouble(featureMin.get(11)));
      double in_flow_max = normalize(Double.parseDouble(dataNetFlow.get(15)),Double.parseDouble(featureMax.get(12)),Double.parseDouble(featureMin.get(12)));
      double in_flow_mean = normalize(Double.parseDouble(dataNetFlow.get(17)),Double.parseDouble(featureMax.get(14)),Double.parseDouble(featureMin.get(14)));
      double in_flow_std = normalize(Double.parseDouble(dataNetFlow.get(18)),Double.parseDouble(featureMax.get(15)),Double.parseDouble(featureMin.get(15)));
      double in_flow_IORatio = normalize(Double.parseDouble(dataNetFlow.get(21)),Double.parseDouble(featureMax.get(18)),Double.parseDouble(featureMin.get(18)));

      for(int centerCanopy=0;(centerCanopy<dataCenterNetFlow.size());centerCanopy=centerCanopy+25)
      {
        double forNumOfPkt = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+3)),Double.parseDouble(featureMax.get(0)),Double.parseDouble(featureMin.get(0)));
        double forNumOfByte = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+4)),Double.parseDouble(featureMax.get(1)),Double.parseDouble(featureMin.get(1)));
        double for_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+5)),Double.parseDouble(featureMax.get(2)),Double.parseDouble(featureMin.get(2)));
        double for_min = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+6)),Double.parseDouble(featureMax.get(3)),Double.parseDouble(featureMin.get(3)));
        double for_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+7)),Double.parseDouble(featureMax.get(4)),Double.parseDouble(featureMin.get(4)));
        double bacNumOfPkt =normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+9)),Double.parseDouble(featureMax.get(6)),Double.parseDouble(featureMin.get(6)));
        double bac_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+10)),Double.parseDouble(featureMax.get(7)),Double.parseDouble(featureMin.get(7)));
        double bac_min = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+11)),Double.parseDouble(featureMax.get(8)),Double.parseDouble(featureMin.get(8)));
        double bac_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+12)),Double.parseDouble(featureMax.get(9)),Double.parseDouble(featureMin.get(9)));
        double flowNumOfByte = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+14)),Double.parseDouble(featureMax.get(11)),Double.parseDouble(featureMin.get(11)));
        double flow_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+15)),Double.parseDouble(featureMax.get(12)),Double.parseDouble(featureMin.get(12)));
        double flow_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+17)),Double.parseDouble(featureMax.get(14)),Double.parseDouble(featureMin.get(14)));
        double flow_std = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+18)),Double.parseDouble(featureMax.get(15)),Double.parseDouble(featureMin.get(15)));
        double flow_IORatio = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+21)),Double.parseDouble(featureMax.get(18)),Double.parseDouble(featureMin.get(18)));

        double distance = getDistance(forNumOfPkt,forNumOfByte,for_max,for_min,for_mean,bacNumOfPkt,bac_max,bac_min,bac_mean,flowNumOfByte,flow_max,flow_mean,flow_std,flow_IORatio,in_forNumOfPkt,in_forNumOfByte,in_for_max,in_for_min,in_for_mean,in_bacNumOfPkt,in_bac_max,in_bac_min,in_bac_mean,in_flowNumOfByte,in_flow_max,in_flow_mean,in_flow_std,in_flow_IORatio);
        //String tag;
        if(distance<t1)
        {
          tag = tag + dataCenterNetFlow.get(centerCanopy+24)+" ";
        }//end if (distance<0.5)   
      }//end for
      String outStrings = "";
      for(int count = 0;count<=24;count++)
      {
        outStrings=outStrings+"\t"+dataNetFlow.get(count); // Initial variable for Textwritable
      }
      outStrings  = outStrings + tag;
      context.write(NullWritable.get(),new Text(outStrings));      
   }//end of map
  }// end of map2 

   
  public static class Reduce2  extends Reducer<NullWritable,Text,NullWritable,Text> 
  {
    private IntWritable result = new IntWritable();
    private Text word = new Text();
    
    public void reduce(Text key, Text values,Context context) throws IOException, InterruptedException
    {
      ArrayList<Double> tempx = new ArrayList<Double>();
      //ArrayList<Double> tempy = new ArrayList<Double>();    
      int sum = 0;
      StringTokenizer stk = new StringTokenizer(values.toString());
      String dataNetFlow = "";
      while(stk.hasMoreTokens())
      {
        dataNetFlow = dataNetFlow + stk.nextToken();
      }

      context.write(NullWritable.get(),new Text(dataNetFlow));
    }
  }  

  public static class Map extends Mapper<Object,Text,IntWritable,Text>
  {
      private Text word = new Text();

      ArrayList<String> dataCenterNetFlow = new ArrayList<String>();

      ArrayList<String> featureMax = new ArrayList<String>();
      ArrayList<String> featureMin = new ArrayList<String>();

    @Override
    public void setup(Context context)throws IOException
    {
      Path pt = new Path("canopyBotnet/standardization/part-r-00000");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;

      line = br.readLine();
      StringTokenizer stk = new StringTokenizer(line);
      String temp0 = stk.nextToken();
 
      while(stk.hasMoreTokens())
      {
        featureMax.add(stk.nextToken());
      }
      
      line = br.readLine();
      stk = new StringTokenizer(line);
      temp0 = stk.nextToken();

      while(stk.hasMoreTokens())
       {
        featureMin.add(stk.nextToken());
      }
      
      br.close();
    }      

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

	   ArrayList<String> dataNetFlow = new ArrayList<String>(); //cache variable
		
	   StringTokenizer itr = new StringTokenizer(value.toString());//prase the string from map
	   int countToken = 0;//check flag
	 
	   while(itr.hasMoreTokens())
     {
        dataNetFlow.add(itr.nextToken()); // Add 24 features to the cache variable
     }
     ////////////////////////////////////// if the center is empty then add the first one on it ///////////////////////////////////////
	  if(dataCenterNetFlow.isEmpty()==true)
	  {

      for(int count = 0;count<=24;count++)
      {
        dataCenterNetFlow.add(dataNetFlow.get(count)); // add to Center Points
      }

      String outStrings = dataNetFlow.get(0) ;

      for(int count = 1;count<=24;count++)
      {
        outStrings=outStrings+"\t"+dataNetFlow.get(count); // Initial variable for Textwritable
      }

      for(int count = 0;count<=24;count++)
      {
        dataNetFlow.remove(0); // remove the cache
        
      }
      dataNetFlow.clear();
      context.write(new IntWritable(1),new Text(outStrings));
	  }
	  else
	  {
  		while(dataNetFlow.isEmpty()!=true)
  		{

          double in_forNumOfPkt =  normalize(Double.parseDouble(dataNetFlow.get(3)),Double.parseDouble(featureMax.get(0)),Double.parseDouble(featureMin.get(0)));
          double in_forNumOfByte =  normalize(Double.parseDouble(dataNetFlow.get(4)),Double.parseDouble(featureMax.get(1)),Double.parseDouble(featureMin.get(1)));
          double in_for_max = normalize(Double.parseDouble(dataNetFlow.get(5)),Double.parseDouble(featureMax.get(2)),Double.parseDouble(featureMin.get(2)));
          double in_for_min = normalize(Double.parseDouble(dataNetFlow.get(6)),Double.parseDouble(featureMax.get(3)),Double.parseDouble(featureMin.get(3)));
          double in_for_mean = normalize(Double.parseDouble(dataNetFlow.get(7)),Double.parseDouble(featureMax.get(4)),Double.parseDouble(featureMin.get(4)));
          double in_bacNumOfPkt = normalize(Double.parseDouble(dataNetFlow.get(9)),Double.parseDouble(featureMax.get(6)),Double.parseDouble(featureMin.get(6)));
          double in_bac_max = normalize(Double.parseDouble(dataNetFlow.get(10)),Double.parseDouble(featureMax.get(7)),Double.parseDouble(featureMin.get(7)));
          double in_bac_min =  normalize(Double.parseDouble(dataNetFlow.get(11)),Double.parseDouble(featureMax.get(8)),Double.parseDouble(featureMin.get(8)));
          double in_bac_mean = normalize(Double.parseDouble(dataNetFlow.get(12)),Double.parseDouble(featureMax.get(9)),Double.parseDouble(featureMin.get(9)));
          double in_flowNumOfByte = normalize(Double.parseDouble(dataNetFlow.get(14)),Double.parseDouble(featureMax.get(11)),Double.parseDouble(featureMin.get(11)));
          double in_flow_max = normalize(Double.parseDouble(dataNetFlow.get(15)),Double.parseDouble(featureMax.get(12)),Double.parseDouble(featureMin.get(12)));
          double in_flow_mean = normalize(Double.parseDouble(dataNetFlow.get(17)),Double.parseDouble(featureMax.get(14)),Double.parseDouble(featureMin.get(14)));
          double in_flow_std = normalize(Double.parseDouble(dataNetFlow.get(18)),Double.parseDouble(featureMax.get(15)),Double.parseDouble(featureMin.get(15)));
          double in_flow_IORatio = normalize(Double.parseDouble(dataNetFlow.get(21)),Double.parseDouble(featureMax.get(18)),Double.parseDouble(featureMin.get(18)));

  			for(int centerCanopy=0;(centerCanopy<dataCenterNetFlow.size()&&dataNetFlow.isEmpty()!=true);centerCanopy=centerCanopy+25)
  			{
  				/*double distance = getDistance(dataCenterNetFlow.get(centerCanopy+),dataCenterNetFlow.get(centerCanopy+1),dataNetFlow.get(0),dataNetFlow.get(1));*/
          double forNumOfPkt = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+3)),Double.parseDouble(featureMax.get(0)),Double.parseDouble(featureMin.get(0)));
          double forNumOfByte = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+4)),Double.parseDouble(featureMax.get(1)),Double.parseDouble(featureMin.get(1)));
          double for_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+5)),Double.parseDouble(featureMax.get(2)),Double.parseDouble(featureMin.get(2)));
          double for_min = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+6)),Double.parseDouble(featureMax.get(3)),Double.parseDouble(featureMin.get(3)));
          double for_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+7)),Double.parseDouble(featureMax.get(4)),Double.parseDouble(featureMin.get(4)));
          double bacNumOfPkt =normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+9)),Double.parseDouble(featureMax.get(6)),Double.parseDouble(featureMin.get(6)));
          double bac_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+10)),Double.parseDouble(featureMax.get(7)),Double.parseDouble(featureMin.get(7)));
          double bac_min = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+11)),Double.parseDouble(featureMax.get(8)),Double.parseDouble(featureMin.get(8)));
          double bac_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+12)),Double.parseDouble(featureMax.get(9)),Double.parseDouble(featureMin.get(9)));
          double flowNumOfByte = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+14)),Double.parseDouble(featureMax.get(11)),Double.parseDouble(featureMin.get(11)));
          double flow_max = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+15)),Double.parseDouble(featureMax.get(12)),Double.parseDouble(featureMin.get(12)));
          double flow_mean = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+17)),Double.parseDouble(featureMax.get(14)),Double.parseDouble(featureMin.get(14)));
          double flow_std = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+18)),Double.parseDouble(featureMax.get(15)),Double.parseDouble(featureMin.get(15)));
          double flow_IORatio = normalize(Double.parseDouble(dataCenterNetFlow.get(centerCanopy+21)),Double.parseDouble(featureMax.get(18)),Double.parseDouble(featureMin.get(18)));

  /*        double in_forNumOfPkt =  Double.parseDouble(dataNetFlow.get(3));
          double in_forNumOfByte =  Double.parseDouble(dataNetFlow.get(4));
          double in_for_max = Double.parseDouble(dataNetFlow.get(5));
          double in_for_min = Double.parseDouble(dataNetFlow.get(6));
          double in_for_mean = Double.parseDouble(dataNetFlow.get(7));
          double in_bacNumOfPkt = Double.parseDouble(dataNetFlow.get(9));
          double in_bac_max = Double.parseDouble(dataNetFlow.get(10));
          double in_bac_min =  Double.parseDouble(dataNetFlow.get(11));
          double in_bac_mean = Double.parseDouble(dataNetFlow.get(12));
          double in_flowNumOfByte = Double.parseDouble(dataNetFlow.get(14));
          double in_flow_max = Double.parseDouble(dataNetFlow.get(15));
          double in_flow_mean = Double.parseDouble(dataNetFlow.get(17));
          double in_flow_std = Double.parseDouble(dataNetFlow.get(18));
          double in_flow_IORatio = Double.parseDouble(dataNetFlow.get(21));*/

          double distance = getDistance(forNumOfPkt,forNumOfByte,for_max,for_min,for_mean,bacNumOfPkt,bac_max,bac_min,bac_mean,flowNumOfByte,flow_max,flow_mean,flow_std,flow_IORatio,in_forNumOfPkt,in_forNumOfByte,in_for_max,in_for_min,in_for_mean,in_bacNumOfPkt,in_bac_max,in_bac_min,in_bac_mean,in_flowNumOfByte,in_flow_max,in_flow_mean,in_flow_std,in_flow_IORatio);

  				if(distance<t2)
  				{
            for(int count = 0;count<=24;count++)
            {
              dataNetFlow.remove(0); // remove the cache
            }
  /*          while(dataNetFlow.isEmpty()!=true)
            {
              dataNetFlow.remove(0);
            }*/
            dataNetFlow.clear();
  				}
  			}
  			if(dataNetFlow.isEmpty()!=true)
  			{
  /*				dataCenterNetFlow.add(dataNetFlow.get(0));
  				dataCenterNetFlow.add(dataNetFlow.get(1));

          String outPoints = dataNetFlow.get(0)+"\t"+dataNetFlow.get(1);
          context.write(new IntWritable(1),new Text(outPoints));
  				//word.set(tempx.get(0).toString());
  				//context.write(word,new DoubleWritable(tempy.get(0)));
  				dataNetFlow.remove(0);
  				dataNetFlow.remove(0);*/
          for(int count = 0;count<=24;count++)
          {
            dataCenterNetFlow.add(dataNetFlow.get(count)); // add to Center Points
          }        

          String outStrings = dataNetFlow.get(0) ;

          for(int count = 1;count<=24;count++)
          {
            outStrings=outStrings+"\t"+dataNetFlow.get(count); // Initial variable for Textwritable
          }
          context.write(new IntWritable(1),new Text(outStrings));
  			 }
  		  }//end of while
	    }// end of else
    } //end of map
  }//end of Mapclass

  public static class Reduce  extends Reducer<IntWritable,Text,Text,Text> 
  {
    private Text wordx = new Text();
    private Text wordy = new Text();
    ArrayList<String> dataCenterNetFlow = new ArrayList<String>();
/*    ArrayList<Double> canopyy = new ArrayList<Double>();*/
      ArrayList<String> featureMax = new ArrayList<String>();
      ArrayList<String> featureMin = new ArrayList<String>();

    @Override
    public void setup(Context context)throws IOException
    {
      Path pt = new Path("canopyBotnet/standardization/part-r-00000");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;

      line = br.readLine();
      StringTokenizer stk = new StringTokenizer(line);
      String temp0 = stk.nextToken();
 
      while(stk.hasMoreTokens())
      {
        featureMax.add(stk.nextToken());
      }
      
      line = br.readLine();
      stk = new StringTokenizer(line);
      temp0 = stk.nextToken();

      while(stk.hasMoreTokens())
       {
        featureMin.add(stk.nextToken());
      }
      br.close();
    }

    
    public void reduce(IntWritable key, Text values,Context context) throws IOException, InterruptedException
    {
      ArrayList<String> dataNetFlow = new ArrayList<String>();

	     StringTokenizer stkvalue = new StringTokenizer(values.toString());

	while(stkvalue.hasMoreTokens())
	{
		//tempx.add(Double.parseDouble(stkkey.nextToken()));
    dataNetFlow.add(stkvalue.nextToken());
		//tempy.add(Double.parseDouble(stkvalue.nextToken()));
	}
  ////////// center vs center /////////////////////////////////////////////////////////////
	if(dataCenterNetFlow.isEmpty()==true)
	{
/*		dataCenterNetFlow.add(dataNetFlow.get(0));
		dataCenterNetFlow.add(dataNetFlow.get(1));
		

		wordx.set(dataNetFlow.get(0).toString());
    wordy.set(dataNetFlow.get(1).toString());
		context.write(wordx,wordy);
		//
		dataNetFlow.remove(0);
		dataNetFlow.remove(0);
		//*/
      for(int count = 0;count<=24;count++)
      {
        dataCenterNetFlow.add(dataNetFlow.get(count)); // add to Center Points
      }

      String outStrings = dataNetFlow.get(0) ;

      for(int count = 1;count<=24;count++)
      {
        outStrings=outStrings+"\t"+dataNetFlow.get(count); // Initial variable for Textwritable
      }

      for(int count = 0;count<=24;count++)
      {
        dataNetFlow.remove(0); // remove the cache
      }
      context.write(new Text(""),new Text(outStrings));
	}
	else
	{
		while(dataNetFlow.isEmpty()!=true)
		{

      double in_forNumOfPkt =  Double.parseDouble(dataNetFlow.get(3));
      double in_forNumOfByte =  Double.parseDouble(dataNetFlow.get(4));
      double in_for_max = Double.parseDouble(dataNetFlow.get(5));
      double in_for_min = Double.parseDouble(dataNetFlow.get(6));
      double in_for_mean = Double.parseDouble(dataNetFlow.get(7));
      double in_bacNumOfPkt = Double.parseDouble(dataNetFlow.get(9));
      double in_bac_max = Double.parseDouble(dataNetFlow.get(10));
      double in_bac_min =  Double.parseDouble(dataNetFlow.get(11));
      double in_bac_mean = Double.parseDouble(dataNetFlow.get(12));
      double in_flowNumOfByte = Double.parseDouble(dataNetFlow.get(14));
      double in_flow_max = Double.parseDouble(dataNetFlow.get(15));
      double in_flow_mean = Double.parseDouble(dataNetFlow.get(17));
      double in_flow_std = Double.parseDouble(dataNetFlow.get(18));
      double in_flow_IORatio = Double.parseDouble(dataNetFlow.get(21));    

			for(int centerCanopy=0;(centerCanopy<dataCenterNetFlow.size()&&dataNetFlow.isEmpty()!=true);centerCanopy=centerCanopy+25)
			{
				//double distance = getDistance(canopyx.get(centerCanopy),canopyx.get(centerCanopy+1),tempx.get(0),tempx.get(1));
        double forNumOfPkt = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+3));
        double forNumOfByte = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+4));
        double for_max = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+5));
        double for_min = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+6));
        double for_mean = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+7));
        double bacNumOfPkt =Double.parseDouble(dataCenterNetFlow.get(centerCanopy+9));
        double bac_max = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+10));
        double bac_min = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+11));
        double bac_mean = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+12));
        double flowNumOfByte = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+14));
        double flow_max = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+15));
        double flow_mean = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+17));
        double flow_std = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+18));
        double flow_IORatio = Double.parseDouble(dataCenterNetFlow.get(centerCanopy+21));       

         double distance = getDistance(forNumOfPkt,forNumOfByte,for_max,for_min,for_mean,bacNumOfPkt,bac_max,bac_min,bac_mean,flowNumOfByte,flow_max,flow_mean,flow_std,flow_IORatio,in_forNumOfPkt,in_forNumOfByte,in_for_max,in_for_min,in_for_mean,in_bacNumOfPkt,in_bac_max,in_bac_min,in_bac_mean,in_flowNumOfByte,in_flow_max,in_flow_mean,in_flow_std,in_flow_IORatio);

				if(distance<t2)
				{
/*					dataNetFlow.remove(0);
					dataNetFlow.remove(0);*/
          for(int count = 0;count<=24;count++)
          {
            dataNetFlow.remove(0); // remove the cache
          }
          dataNetFlow.clear();
				}
			}// end for
			if(dataNetFlow.isEmpty()!=true)
			{
/*				dataCenterNetFlow.add(dataNetFlow.get(0));
				dataCenterNetFlow.add(dataNetFlow.get(1));
				//
        wordx.set(dataNetFlow.get(0).toString());
        wordy.set(dataNetFlow.get(1).toString());
        context.write(wordx,wordy);
			        //
				tempx.remove(0);
				tempx.remove(0);*/
        for(int count = 0;count<=24;count++)
        {
          dataCenterNetFlow.add(dataNetFlow.get(count)); // add to Center Points
        }        

        String outStrings = dataNetFlow.get(0) ;

        for(int count = 1;count<=24;count++)
        {
          outStrings=outStrings+"\t"+dataNetFlow.get(count); // Initial variable for Textwritable
        }
        context.write(new Text(""),new Text(outStrings));        
			}// end if	
		}//end while
	}//end of reducer
}//end of reduce
}

public static double normalize(double feature1,double max, double min)
{
  double normalizeResult = (feature1 - min) / (max - min) * 100;
  return normalizeResult;
}

public static double  getDistance(double point1a,double point1b,double point1c,double point1d,double point1e,double point1f,double point1g,double point1h,double point1i,double point1j,double point1k,double point1l,double point1m,double point1n,double point2a,double point2b,double point2c,double point2d,double point2e,double point2f,double point2g,double point2h,double point2i,double point2j,double point2k,double point2l,double point2m,double point2n)
{
  double tempDistance = Math.sqrt(Math.pow(point1a-point2a,2)+Math.pow(point1b-point2b,2)+Math.pow(point1c-point2c,2)+Math.pow(point1d-point2d,2)+Math.pow(point1e-point2e,2)+Math.pow(point1f-point2f,2)+Math.pow(point1g-point2g,2)+Math.pow(point1h-point2h,2)+Math.pow(point1i-point2i,2)+Math.pow(point1j-point2j,2)+Math.pow(point1k-point2k,2)+Math.pow(point1l-point2l,2)+Math.pow(point1m-point2m,2)+Math.pow(point1n-point2n,2));
  //System.out.println("Inside getDistance\n"+point1x+"\t"+point1y+"\t+"+point2x+"\t"+point2y+"\n");
  return tempDistance;
}

  public static void main(String[] args) throws Exception 
  {
    boolean verbose =false;
	///////////////////////// job 1 ////////////////////////////////////////////
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Canopy_MapReduce_GetStandardization");
    System.out.println("Job> Canopy_MapReduce_B");
    
    FileSystem  fs1 = FileSystem.get(conf);
    job1.setJarByClass(Canopy5.class);
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
    if(fs1.exists(new Path(args[1])))
  	fs1.delete(new Path(args[1]),true);
    
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    System.out.println("intput path-> "+args[0]);
    System.out.println("output path-> "+args[1]);
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    /////////////////// wait job1 done the job 1st ////////////////////////////////
   if(job1.waitForCompletion(true))
    {
       System.out.println("Doing second times mapreduce...");
    }
    else
    {
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
    String line;
/*    while((line = br.readLine())!=null)
    {
      StringTokenizer stk = new StringTokenizer(line);
      if(stk.nextToken().equals("max"))
      {
        System.out.print("max: "+"\t");
        while(stk.hasMoreTokens())
        {
          System.out.print(stk.nextToken()+"\t");
        }
      }
      else if(stk.nextToken().equals("min"))
      {
        System.out.print("min: "+"\t");
        while(stk.hasMoreTokens())
        {
          System.out.print(stk.nextToken()+"\t");
        }
      }
      System.out.println("");
    }*/
      line = br.readLine();
      StringTokenizer stk = new StringTokenizer(line);
      String temp0 = stk.nextToken();
      System.out.println("");
      System.out.print(temp0+"\t");
      while(stk.hasMoreTokens())
      {
        System.out.print(stk.nextToken()+"\t");
      }
      
      line = br.readLine();
      stk = new StringTokenizer(line);
      temp0 = stk.nextToken();
      System.out.println("");
      System.out.print(temp0+"\t");
      while(stk.hasMoreTokens())
       {
        System.out.print(stk.nextToken()+"\t");
      }    
    br.close();
    System.out.println("");
    System.out.println("Done.");
    //
    System.out.println("Doing second times mapreduce...");
///////////////////////////////// job2 ///////////////////////////
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Canopy_MapReduce_Round2");
    System.out.println("Job2> Canopy_MapReduce_C");
    
    FileSystem  fs2 = FileSystem.get(conf2);
    job2.setJarByClass(Canopy5.class);
    job2.setNumReduceTasks(1); 

    //job.setMapperClass(TokenizerMapper.class);
    //job.setReducerClass(IntSumReduce.class);  
    job2.setMapperClass(Map.class);
    job2.setReducerClass(Reduce.class);
    //job.setCombinerClass(IntSumReducer.class);
   // job.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);
    //job.setOutputValueClass(IntWritable.class);
    if(fs2.exists(new Path(args[2])))
    fs2.delete(new Path(args[2]),true);
    
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    System.out.println("intput path-> "+args[0]);
    System.out.println("output path-> "+args[2]);
    //System.exit(job2.waitForCompletion(true) ? 0 : 1);
 /////////////////////////////////////// wait for job 2 done //////////////////////////////////////////////
    if(job2.waitForCompletion(true))
    {
       System.out.println("Doing third times mapreduce...");
    }
    else
    {
       System.out.println("Trouble on second mapreduce");
       System.exit(1);
    }       
 ////////////////////////////// job2 ///////////////////////////////////////////       


    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3,"Canopy_MapReduce iteration");
    System.out.println("Job2> Canopy_MapReduce_D");

    FileSystem fs3 =  FileSystem.get(conf3);
    job3.setJarByClass(Canopy5.class);
    job3.setNumReduceTasks(1);

    job3.setMapperClass(Map2.class);
    job3.setReducerClass(Reduce2.class);
   
    job3.setOutputKeyClass(NullWritable.class);
    job3.setOutputValueClass(Text.class);
    
   if(fs3.exists(new Path(args[3])))
    fs3.delete(new Path(args[3]),true);

   FileInputFormat.addInputPath(job3,new Path(args[0]));
   FileOutputFormat.setOutputPath(job3,new Path(args[3]));

   System.out.println("input path-> "+args[0]);
   System.out.println("output path0> "+args[3]);

   System.exit(job3.waitForCompletion(true)?0:1);

} 
   /////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Phase 0 
  public static class Phase0Mapper extends Mapper<LongWritable, Text, Text, Text>
  {
    // counter
    private int mFeatureInit = 0 ;

    // feature vector max, min value
    private double[] mFVMax = new double[ sMaxMinFeatureNum ] ;
    private double[] mFVMin = new double[ sMaxMinFeatureNum ] ;

    // map intermediate value
    private Text interKey = new Text() ;
    private Text interValue = new Text() ;

    public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
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
        while(stk.hasMoreTokens())
        {
          aFeatures.add(stk.nextToken());
        }
      } 
      catch ( Exception e)
      {
        return;
      }

      for ( int i = 0 ; i < sMaxMinFeatureNum ; i ++ ) 
      {
        try {
          /*double aValue = Double.parseDouble( aFeatures [i + sOffset] );*/
          double  aValue = Double.parseDouble(aFeatures.get(i + sOffset));
          if ( mFeatureInit == 0 ) 
          {
            mFVMax[i] = aValue ;
            mFVMin[i] = aValue ;
          } 
          else 
          {
            if ( aValue > mFVMax[i] )
             { 
              mFVMax[i] = aValue ; 
            }
            if ( aValue < mFVMin[i] ) 
            {
                mFVMin[i] = aValue ;
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
    protected void cleanup ( Context context ) throws IOException, InterruptedException
    {
      StringBuilder maxBuilder = new StringBuilder();
      StringBuilder minBuilder = new StringBuilder();
      for ( int i = 0 ; i < mFVMax.length ; i ++ ) { maxBuilder.append( mFVMax[i] ).append("\t"); }
      for ( int i = 0 ; i < mFVMin.length ; i ++ ) { minBuilder.append( mFVMin[i] ).append("\t"); }

      //Keep codes as clean as possible.
      interKey.set ( "max" ) ;
      interValue.set ( maxBuilder.toString() ) ;
      context.write ( interKey, interValue ) ;

      interKey.set ( "min" ) ;
      interValue.set ( minBuilder.toString() ) ;
      context.write ( interKey, interValue ) ;
    }

  }

  public static class Phase0Reducer extends Reducer<Text, Text, Text, Text>
  {
    private int mFeatureMaxInit = 0;
    private int mFeatureMinInit = 0;

    private double[] mFVMax = new double[ sMaxMinFeatureNum ] ;
    private double[] mFVMin = new double[ sMaxMinFeatureNum ] ;

    // reduce output value
    private Text outputKey = new Text() ;
    private Text outputValue = new Text() ;

    public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
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


      for ( Text val : values ) 
      {
        try 
        {
          String[] aFeatures = val.toString().split("\t");
          int type = 0;
          // decide the max features or min features
          if ( key.toString().equals("max") ) 
          {
            type = TYPE_MAX ;
          }
          else if ( key.toString().equals("min") )
          {
            type = TYPE_MIN ; 
          }

          for( int i = 0 ; i < aFeatures.length ; i++ )
          {
            double aValue = Double.parseDouble( aFeatures[i] );
            if ( type == TYPE_MAX )
             {
              if ( mFeatureMaxInit == 0  ) 
              {
                mFVMax[i] = aValue;
              } 
              else
               {
                if ( mFVMax[i] < aValue )
                 { 
                  mFVMax[i] = aValue;
                 } 
               }
            } else if ( type == TYPE_MIN ) {
              if ( mFeatureMinInit == 0  ) {
                mFVMin[i] = aValue;
              } else {
                if ( mFVMin[i] > aValue ) { mFVMin[i] = aValue; } 
              }
            }
          }// end of inner for-loop
          if ( type == TYPE_MAX )
          {
            mFeatureMaxInit ++;
          }
          else if ( type == TYPE_MIN )
          {
            mFeatureMinInit ++;
          }
        } 
        catch (Exception e)
        {

        } // end of catch
      } //end of for
    }

    @Override
    protected void cleanup ( Context context ) throws IOException, InterruptedException
    {
      StringBuilder maxBuilder = new StringBuilder();
      StringBuilder minBuilder = new StringBuilder();
      for ( int i = 0 ; i < mFVMax.length ; i ++ ) { maxBuilder.append( mFVMax[i] ).append("\t"); }
      for ( int i = 0 ; i < mFVMin.length ; i ++ ) { minBuilder.append( mFVMin[i] ).append("\t"); }

      //IF code is stable, please remember to remove those unpleasure debug messaages.
      outputKey.set( "max" ) ;
      outputValue.set ( maxBuilder.toString() ) ;
      context.write ( outputKey, outputValue ) ;

      outputKey.set( "min" ) ;
      outputValue.set ( minBuilder.toString() ) ;
      context.write ( outputKey, outputValue ) ;
    }
  }   
}