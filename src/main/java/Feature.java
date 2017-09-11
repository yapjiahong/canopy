

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class Feature
{
	double x;
	double y;

	boolean remove = false;
	  
	static int onelineparse = 25;

	static int tp = 0;
	static int proto = 1;
	static int flowid = 2;

	static int f_pkts = 3;
	static int f_byts = 4;
	static int f_maxbyts=5;
	static int f_minbyts=6;
	static int f_meanbyts=7;

	static int b_pkts=8;
	static int b_byts=9;
	static int b_maxbyts=10;
	static int b_minbyts=11;
	static int b_meanbyts=12;

	static int fl_pkts=13;
	static int fl_byts=14;
	static int fl_maxbyts=15;
	static int fl_minbyts=16;
	static int fl_meanbys=17;

	static int fl_stdbys=18;
	static int fl_pktsperms=19;
	static int fl_bytsperms=20;
	static int fl_bytsio=21;
	static int fl_ratio=22;

	static int flowloss = 23;
	 static int serialnum = 24;
	 //==============================
	String timestamp;
	String protocol;
	String flow_id;

	double foward_pkts;
	double foward_byts;
	double foward_maxbyts;
	double foward_minbyts;
	double foward_meanbyts;

	double back_pkts;
	double back_byts;
	double back_maxbyts;
	double back_minbyts;
	double back_meanbyts;

	double flow_pkts;
	double flow_byts;
	double flow_maxbyts;
	double flow_minbyts;
	double flow_meanbys;

	double flow_stdbys;
	double flow_pktsperms;
	double flow_bytsperms;
	double flow_bytsio;
	double flow_ratio;	

	String flow_loss;
	String flow_serial;

	double[] datanormalize = new double[20];

	public Feature()
	{
		foward_pkts = foward_byts = foward_maxbyts = foward_minbyts = foward_meanbyts = 0.0;
		back_pkts = back_byts = back_maxbyts = back_minbyts = back_meanbyts = 0.0;
		flow_pkts = flow_byts = flow_maxbyts = flow_minbyts = flow_meanbys = 0.0;
		flow_stdbys = flow_pktsperms = flow_bytsperms = flow_bytsio = flow_ratio = 0.0;
		remove = false;
	}

	public Feature(String inputXY)
	{
		String[] data = inputXY.toString().split("\t");
		System.out.println(data.length);
		if(data.length==onelineparse)
		{
	      this.timestamp = data[tp];
	      this.protocol = data[proto];
	      this.flow_id = data[flowid];

	      this.foward_pkts = Double.parseDouble(data[f_pkts]);
	      this.foward_byts = Double.parseDouble(data[f_byts]);
	      this.foward_maxbyts = Double.parseDouble(data[f_maxbyts]);
	      this.foward_minbyts = Double.parseDouble(data[f_minbyts]);
	      this.foward_meanbyts = Double.parseDouble(data[f_meanbyts]);

	      this.back_pkts = Double.parseDouble(data[b_pkts]);
	      this.back_byts = Double.parseDouble(data[b_byts]);
	      this.back_maxbyts = Double.parseDouble(data[b_maxbyts]);
	      this.back_minbyts = Double.parseDouble(data[b_minbyts]);
	      this.back_meanbyts = Double.parseDouble(data[b_meanbyts]);

	      this.flow_pkts = Double.parseDouble(data[fl_pkts]);
	      this.flow_byts = Double.parseDouble(data[fl_byts]);
	      this.flow_maxbyts = Double.parseDouble(data[fl_maxbyts]);
	      this.flow_minbyts = Double.parseDouble(data[fl_minbyts]);
	      this.flow_meanbys = Double.parseDouble(data[fl_meanbys]);

	      this.flow_stdbys = Double.parseDouble(data[fl_stdbys]);
	      this.flow_pktsperms = Double.parseDouble(data[fl_pktsperms]);
	      this.flow_bytsperms = Double.parseDouble(data[fl_bytsperms]);
	      this.flow_bytsio = Double.parseDouble(data[fl_bytsio]);
	      this.flow_ratio = Double.parseDouble(data[fl_ratio]);  

	      this.flow_loss = data[flowloss];
	      this.flow_serial = data[serialnum]; 		
		}
		else if(data.length==(onelineparse+1))
		{
	      this.timestamp = data[tp+1];
	      this.protocol = data[proto+1];
	      this.flow_id = data[flowid+1];

	      this.foward_pkts = Double.parseDouble(data[f_pkts+1]);
	      this.foward_byts = Double.parseDouble(data[f_byts+1]);
	      this.foward_maxbyts = Double.parseDouble(data[f_maxbyts+1]);
	      this.foward_minbyts = Double.parseDouble(data[f_minbyts+1]);
	      this.foward_meanbyts = Double.parseDouble(data[f_meanbyts+1]);

	      this.back_pkts = Double.parseDouble(data[b_pkts+1]);
	      this.back_byts = Double.parseDouble(data[b_byts+1]);
	      this.back_maxbyts = Double.parseDouble(data[b_maxbyts+1]);
	      this.back_minbyts = Double.parseDouble(data[b_minbyts+1]);
	      this.back_meanbyts = Double.parseDouble(data[b_meanbyts+1]);

	      this.flow_pkts = Double.parseDouble(data[fl_pkts+1]);
	      this.flow_byts = Double.parseDouble(data[fl_byts+1]);
	      this.flow_maxbyts = Double.parseDouble(data[fl_maxbyts+1]);
	      this.flow_minbyts = Double.parseDouble(data[fl_minbyts+1]);
	      this.flow_meanbys = Double.parseDouble(data[fl_meanbys+1]);

	      this.flow_stdbys = Double.parseDouble(data[fl_stdbys+1]);
	      this.flow_pktsperms = Double.parseDouble(data[fl_pktsperms+1]);
	      this.flow_bytsperms = Double.parseDouble(data[fl_bytsperms+1]);
	      this.flow_bytsio = Double.parseDouble(data[fl_bytsio+1]);
	      this.flow_ratio = Double.parseDouble(data[fl_ratio+1]);  

	      this.flow_loss = data[flowloss+1];
	      this.flow_serial = data[serialnum+1];
		}
		else
		{

		}	

	}

	public Feature(Feature p)
	{
        this.timestamp = p.timestamp;
        this.protocol = p.protocol;
        this.flow_id = p.flow_id;

        this.foward_pkts = p.foward_pkts;
        this.foward_byts = p.foward_maxbyts;
        this.foward_maxbyts = p.foward_maxbyts;
        this.foward_minbyts = p.foward_minbyts;
        this.foward_meanbyts = p.foward_minbyts;

        this.back_pkts =  p.back_pkts;
        this.back_byts =  p.back_byts;
        this.back_maxbyts = p.back_maxbyts;
        this.back_minbyts = p.back_minbyts;
        this.back_meanbyts = p.back_meanbyts;

        this.flow_pkts = p.flow_pkts;
        this.flow_byts = p.flow_byts;
        this.flow_maxbyts = p.flow_maxbyts;
        this.flow_minbyts = p.flow_minbyts;
        this.flow_meanbys = p.flow_meanbys;

        this.flow_stdbys = p.flow_stdbys;
        this.flow_pktsperms = p.flow_pktsperms;
        this.flow_bytsperms = p.flow_bytsperms;
        this.flow_bytsio = p.flow_bytsio;
        this.flow_ratio = p.flow_ratio;  

        this.flow_loss = p.flow_loss;
        this.flow_serial = p.flow_serial;   		
	}

	public void remove(boolean Remove)
	{
		this.remove = Remove;
	}

	public boolean getRemove()
	{
		return this.remove;
	}
 	public  double  getDistance(Feature tempoints)
	{
	  //double tempDistance = Math.sqrt(Math.pow(this.x-tempoints.x,2)+Math.pow(this.y-tempoints.y,2));
	  double ans = 0.0;
	  for(int count=0;count<datanormalize.length;count++)
	  {
	  	ans = ans + Math.pow(this.datanormalize[count]-tempoints.datanormalize[count],2);
	  }
	   double tempDistance = Math.sqrt(ans);
	  return tempDistance;
	}

	public String getString()
	{
		String s = timestamp+"\t";
		s = s +protocol+"\t";
		s = s +flow_id+"\t";

		s = s + String.valueOf(foward_pkts)+"\t";
		s = s + String.valueOf(foward_byts)+"\t";
		s = s + String.valueOf(foward_maxbyts)+"\t";
		s = s + String.valueOf(foward_minbyts)+"\t";
		s = s + String.valueOf(foward_meanbyts)+"\t";

		s = s + String.valueOf(back_pkts)+"\t";
		s = s + String.valueOf(back_byts)+"\t";
		s = s + String.valueOf(back_maxbyts)+"\t";
		s = s + String.valueOf(back_minbyts)+"\t";
		s = s + String.valueOf(back_meanbyts)+"\t";

		s = s + String.valueOf(flow_pkts)+"\t";
		s = s + String.valueOf(flow_byts)+"\t";
		s = s + String.valueOf(flow_maxbyts)+"\t";
		s = s + String.valueOf(flow_minbyts)+"\t";
		s = s + String.valueOf(flow_meanbys)+"\t";

		s = s + String.valueOf(flow_stdbys)+"\t";
		s = s + String.valueOf(flow_pktsperms)+"\t";
 		s = s + String.valueOf(flow_bytsperms)+"\t";
 		s = s + String.valueOf(flow_bytsio)+"\t";
 		s = s + String.valueOf(flow_ratio)+"\t";

 		s = s +flow_loss+"\t";
 		s = s +flow_serial;	
		return s;
	}

	public void setupString(String value)
	{
		String[] temp = value.split("\t");

		this.timestamp = temp[tp+1];
		this.protocol = temp[proto+1];
		this.flow_id = temp[flowid+1];

		this.foward_pkts = Double.parseDouble(temp[f_pkts+1]);
		this.foward_byts = Double.parseDouble(temp[f_byts+1]);
		this.foward_maxbyts = Double.parseDouble(temp[f_maxbyts+1]);
		this.foward_minbyts = Double.parseDouble(temp[f_minbyts+1]);
		this.foward_meanbyts = Double.parseDouble(temp[f_meanbyts+1]);

		this.back_pkts = Double.parseDouble(temp[b_pkts+1]);
		this.back_byts = Double.parseDouble(temp[b_byts+1]);
		this.back_maxbyts = Double.parseDouble(temp[b_maxbyts+1]);
		this.back_minbyts = Double.parseDouble(temp[b_minbyts+1]);
		this.back_meanbyts = Double.parseDouble(temp[b_meanbyts+1]);

		this.flow_pkts = Double.parseDouble(temp[fl_pkts+1]);
		this.flow_byts = Double.parseDouble(temp[fl_byts+1]);
		this.flow_maxbyts = Double.parseDouble(temp[fl_maxbyts+1]);
		this.flow_minbyts = Double.parseDouble(temp[fl_minbyts+1]);
		this.flow_meanbys = Double.parseDouble(temp[fl_meanbys+1]);

		this.flow_stdbys = Double.parseDouble(temp[fl_stdbys+1]);
		this.flow_pktsperms = Double.parseDouble(temp[fl_pktsperms+1]);
		this.flow_bytsperms = Double.parseDouble(temp[fl_bytsperms+1]);
		this.flow_bytsio = Double.parseDouble(temp[fl_bytsio+1]);
		this.flow_ratio = Double.parseDouble(temp[fl_ratio+1]);  

		this.flow_loss = temp[flowloss+1];
		this.flow_serial = temp[serialnum+1];   	

	}
	
	public void doNormalize(Normalize normalize)
	{
		datanormalize[0] = (foward_pkts - normalize.min[0]) / (normalize.max[0] - normalize.min[0]) * 100; 
		datanormalize[1] = (foward_byts - normalize.min[1]) / (normalize.max[1] - normalize.min[1]) * 100;
		datanormalize[2] = (foward_maxbyts - normalize.min[2]) / (normalize.max[2] - normalize.min[2]) * 100;
		datanormalize[3] = (foward_minbyts- normalize.min[3]) / (normalize.max[3] - normalize.min[3]) * 100;
		datanormalize[4] = (foward_meanbyts- normalize.min[4]) / (normalize.max[4] - normalize.min[4]) * 100;
		datanormalize[5] = (back_byts - normalize.min[6]) / (normalize.max[6] - normalize.min[6]) * 100;
		datanormalize[6] = (back_maxbyts- normalize.min[7]) / (normalize.max[7] - normalize.min[7]) * 100;
		datanormalize[7] = (back_minbyts- normalize.min[8]) / (normalize.max[8] - normalize.min[8]) * 100;
		datanormalize[8] = (back_meanbyts- normalize.min[9]) / (normalize.max[9] - normalize.min[9]) * 100;
		datanormalize[9] = (flow_byts- normalize.min[11]) / (normalize.max[11] - normalize.min[11]) * 100;
		datanormalize[10] = (flow_maxbyts- normalize.min[12]) / (normalize.max[12] - normalize.min[12]) * 100;
		datanormalize[11] = (flow_meanbys- normalize.min[14]) / (normalize.max[14] - normalize.min[14]) * 100;
		datanormalize[12] = (flow_stdbys- normalize.min[15]) / (normalize.max[15] - normalize.min[15]) * 100;
		datanormalize[13] = (flow_meanbys- normalize.min[18]) / (normalize.max[18] - normalize.min[18]) * 100;                
	}

	public void addTag(Feature f)
	{
		this.flow_serial = this.flow_serial + " " + f.flow_serial;
	}	
}
