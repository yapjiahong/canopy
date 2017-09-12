  
public class Normalize
{ 

  static int onelineparse = 20;
  static int f_pkts = 0;
  static int f_byts = 1;
  static int f_maxbyts=2;
  static int f_minbyts=3;
  static int f_meanbyts=4;

  static int b_pkts=5;
  static int b_byts=6;
  static int b_maxbyts=7;
  static int b_minbyts=8;
  static int b_meanbyts=9;

  static int fl_pkts=10;
  static int fl_byts=11;
  static int fl_maxbyts=12;
  static int fl_minbyts=13;
  static int fl_meanbys=14;

  static int fl_stdbys=15;
  static int fl_pktsperms=16;
  static int fl_bytsperms=17;
  static int fl_bytsio=18;
  static int fl_ratio=19;

////////////////////////////////////
  Double[] max = new Double[onelineparse];
  Double[] min = new Double[onelineparse];

  public Normalize()
  {

  }
    
  public void fileString(String input)
  {
    String[] data = input.toString().split("\t");
    if(data.length==onelineparse+1)
    {
     if(data[0].equals("max")==true)
     {
      for(int count = 0;count<onelineparse;count++)
      {
        this.max[count] = Double.parseDouble(data[count+1]);
      }
     }
     else if(data[0].equals("min")==true)
     {
      for(int count = 0;count<onelineparse;count++)
      {
        this.min[count] = Double.parseDouble(data[count+1]);
      }
     }       
    }
  }

  public String getPrint()
  {
   String s="max\t";
   for(Double temp : max)
   {
      s = s + String.valueOf(temp)+"\t";
   }

   String y="min\t";
   for(Double temp : min)
   {
      y = y + String.valueOf(temp)+"\t";
   }
   s = s +"\n"+y;
   return s;
  }
}