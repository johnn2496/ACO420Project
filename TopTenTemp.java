import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.List;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopTenTemp extends Configured implements Tool 
{	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
 	{	
		private static int STATION_ID	= 0;
		private static int ZIPCODE		= 1;
		private static int LAT			= 2;
		private static int LONG			= 3;
		private static int TEMP			= 4;
		private static int PERCIP		= 5;
		private static int HUMID		= 6;
		private static int YEAR			= 7;
		private static int MONTH		= 8;
		
		LinkedList<String> TopTenList;
		protected void setup (Context context) throws IOException, InterruptedException
		{
			TopTenList = new LinkedList<String>();
		}
		
 		public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException
 		{
 			//StationID | Zipcode |  Lat      |  Lon       |  Temp   | Percip | Humid | Year | Month 
 			String[] element = new String[9];
 			StringTokenizer st = new StringTokenizer(inputValue.toString());
 			
 			for(int i = 0; i < element.length; i++)
 			{
	 			if(st.hasMoreTokens())
	 			{
	 				element[i] = st.nextToken();
	 				if(i == 0 && element[i].equals("StationID"))
	 				{
	 					return;
	 				}
	 			}
				else 
					return;
 			} //end for
 			
 			//Through the list of temperature from the dataset, compare each temperature value.
 			double tempVal = Double.parseDouble(element[TEMP]);
 			Iterator<String> itr = TopTenList.iterator();
 			
 			while(itr.hasNext())
 			{
 				String listValue = itr.next();
 				StringTokenizer tempstr = new StringTokenizer(listValue);
 				//If the temporary data is greater than the data of current tokenizer 
 				if(tempVal > Double.parseDouble(tempstr.nextToken()))
 				{
 					//Add the temporary data into the list at the position where the new value is greater than the old value.
 					TopTenList.add(TopTenList.indexOf(listValue), element[TEMP] + " " + element[YEAR] + " " + element[MONTH] + " " + element[ZIPCODE]);
 					//Make sure the list only contains the most top 10 values.
 					if(TopTenList.size() > 10)
 					{
 						//Remove the minimum value from the list.
 						TopTenList.removeLast();
 					}
 					break;
 				}
 			}
 			//If list is empty, then comparison can't be done, need to add initial value for starting comparison
 			if(TopTenList.size() == 0)
 			{
 				TopTenList.add(element[TEMP] + " " + element[YEAR] + " " + element[MONTH] + " " + element[ZIPCODE]);
 			}	
 		}//end map
 		
 		//format the output for the reducer pushing only the top ten in the map to the reducer
 		public void cleanup (Context context) throws IOException, InterruptedException
 		{
			Iterator<String> itr = TopTenList.iterator();
			while (itr.hasNext())
			{
				String listValue = itr.next();
				StringTokenizer st = new StringTokenizer(listValue);
				String[] element = new String[4];
				for(int i = 0; i < element.length; i++)
				{
					element[i] = st.nextToken();
	 			}
				//sends key value pair to reducer: (ZIPCODE, (TEMP YEAR MONTH))
				context.write(new Text(element[3]), new Text(element[0] + " " + element[1] + " " + element[2]));
			}
		}
	}//end MyMapper
 		
	public static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		//Create a linked list to store the top ten temperature
		LinkedList<String> topTempList;
		
		protected void setup (Context context) throws IOException, InterruptedException
		{
			topTempList = new LinkedList<String>();
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Iterator<Text> valItr = values.iterator();
			
			while(valItr.hasNext())
			{
				String value = valItr.next().toString();
				StringTokenizer st1 = new StringTokenizer(value);
				double tempValue = Double.parseDouble(st1.nextToken());
				Iterator<String> itr = topTempList.iterator();
				while (itr.hasNext())
				{
					String listValue = itr.next();
					StringTokenizer st2 = new StringTokenizer(listValue);
					//If the temporary value's temperature greater than the temper value at next token, add that value to the list
					if (tempValue > Double.parseDouble(st2.nextToken()))
					{
						topTempList.add(topTempList.indexOf(listValue), (value + " " + key));
						if (topTempList.size() > 10)
						{
							topTempList.removeLast();
						}
						break;
					}
				}
				if (topTempList.size() == 0)
				{
					topTempList.add(value + " " + key);
					break;
				}
			}
		}//end reduce
		
		//Formats the output from (value, key) to (key, value) as (ZIPCODE YEAR MONTH, (TEMP))
		public void cleanup (Context context) throws IOException, InterruptedException
		{
			Iterator<String> itr = topTempList.iterator();
			while (itr.hasNext())
			{
				String listValue = itr.next();
				StringTokenizer st = new StringTokenizer(listValue);
				String[] element = new String[4];
				for(int i = 0; i < element.length; i++)
				{
					element[i] = st.nextToken();
	 			}
				context.write(new Text(element[3] + " " + element[2] + " " + element[1]), new DoubleWritable(Double.parseDouble(element[0])));
			}
		}
	}//end MyReducer
 	
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new TopTenTemp(), args);
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception 
	{
		if(args.length != 3)
		{
			System.out.println("Usage: bin/hadoop jar ACO420Project1.jar TopTenTemp <input directory> <ouput directory> <number of reduces>");
			System.out.println("args length incorrect, length: " + args.length);
			return -1;
		}
		int numReduces;
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		try
		{
			numReduces 	= new Integer(args[2]);
			System.out.println("number reducers set to: " + numReduces);
		}
		catch(NumberFormatException e)
		{
			System.out.println("Usage: bin/hadoop jar ACO420Project1.jar TopTenTemp <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: number of reduces not a type integer");
			return -1;
		}
		
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(inputPath))
		{
			System.out.println("Usage: bin/hadoop jar ACO420Project1.jar TopTenTemp <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: Input Directory Does Not Exist");
			System.out.println("Invalid input Path: " + inputPath.toString());
			return -1;
		}
		
		if(fs.exists(outputPath))
		{
			System.out.println("Usage: bin/hadoop jar ACO420Project1.jar TopTenTemp <input directory> <ouput directory> <number of reduces>");
			System.out.println("Error: Output Directory Already Exists");
			System.out.println("Please delete or specifiy different output directory");
			return -1;
		}
		
		Job job = new Job(conf, "MapReduceShell Test");
		
		job.setNumReduceTasks(numReduces);
		job.setJarByClass(TopTenTemp.class);
		
		//sets mapper class
		job.setMapperClass(MyMapper.class);
		
		//sets map output key/value types
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
		//Set Reducer class
	    job.setReducerClass(MyReducer.class);
	    
	    // specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);	
		
		//sets Input format
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    // specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		
		job.waitForCompletion(true);
		return 0;
	}
	
}
