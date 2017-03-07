/*
	Name	: Rajat Dhanuka
	Course	: DBMS Models & Implementation
	Year	: 2014
*/
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Weight {
	
	// Creating a Hash map for states and gender
	private static HashMap<String, String> stateMap = new HashMap<String, String>();
	private static HashMap<String, String> genderMap = new HashMap<String, String>();
	static{
		
	// Filtering the states
		stateMap.put("06","California");stateMap.put("08","Colorado");stateMap.put("12","Florida");stateMap.put("42","Pennsylvania");stateMap.put("48","Texas");
		stateMap.put("50","Vermont");stateMap.put("53","Washington");		
	
	// Viewing based on Gender
		genderMap.put("1", "M");
		genderMap.put("2", "F");
	}
	
	public static String getStateName(String val) {
		return stateMap.get(val);
	}
	
	public static String getGender(String val) {
		if (genderMap.get(val) != null){
			return genderMap.get(val);
		}else{
			throw new IllegalArgumentException("Gender is Not Specified");
		}
	}
	
	public static class WeightMapper extends Mapper < LongWritable, Text, Text, DoubleWritable > {
		public void map(LongWritable inputKey, Text inputValue, Context inputContext) throws IOException, InterruptedException {

			//We will input the data set file 
			String textAsInput = inputValue.toString();
			
			//SERIALNO(0), STATE(5), SEX(69), WEIGHT(PWGTP 7)
			//200900000001   6 		 1(Male) 	170 
			//State Names: 6) California, 8) Colorado, 53) Washington, 48) Texas, 42) Pennsylvania, 50) Vermont, and 12) Florida
			boolean valid = false;

			
			try{
			//Checking if it is a valid data tuple
			if (Character.isDigit(textAsInput.charAt(0))) {
				String toReducer = null;
				String inputArray[] = textAsInput.split(",");  	// To use comma as delimitters
				DoubleWritable weight = new DoubleWritable(Double.parseDouble(inputArray[7]));
				
				int state = Integer.parseInt(inputArray[5]);
				if ( state == 6 || state == 8 || state == 12 || state == 42 || state == 48 || state == 50 || state == 53) { 
				// Considering only valid states we need to analyze
					valid = true;
				}
				
				if(valid){
				toReducer = inputArray[0].substring(0, 4).concat(" | ").concat(getGender(inputArray[69])).concat(" | ").concat(getStateName(inputArray[5])).concat(" | ");
					
					//OutputKey - : Year | Gender | State |, OutputValue: Weight

					inputContext.write(new Text(toReducer), weight);
				}
			}
			} catch (Exception e) {	
			}
		}
	}

	public static class WeightReducer extends Reducer < Text, DoubleWritable, Text, DoubleWritable > {
		public void reduce(Text key, Iterable < DoubleWritable > values, Context context) throws IOException, InterruptedException {

			//InputKey: Year | Gender | State |, Input value: Weight

			int count = 0;
			double totalWeight = 0.0;

			for (DoubleWritable data : values) {
				count++;											
				totalWeight += data.get(); //Adding current weight to total weight
			}

			//Finding average of all weights
			double avgWeight = (double)(totalWeight / count);

			//OutputKey: Year | State | Sex |, Output value: avgWeight
			context.write(key, new DoubleWritable(avgWeight));

		}
	}

	
	public static void main(String[] args) throws Exception {

		try {
			Configuration conf = new Configuration();

			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf, "CalculateAverageWeight");
			job1.setJarByClass(Weight.class);
			
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(DoubleWritable.class);
			
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(DoubleWritable.class);
			
			job1.setMapperClass(WeightMapper.class);
			job1.setReducerClass(WeightReducer.class);
			
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));
			
			job1.waitForCompletion(true);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
