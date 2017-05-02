package join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IntWritable;



public class FinalPhotoMapper extends Mapper<Object, Text, IntWritable,Text> {
	
	
	private Text valueOut = new Text();
	private IntWritable keyOut = new IntWritable();
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		
		String[] dataArray = value.toString().split("\t"); 
		if (dataArray.length < 2){ 
			return; 
		}
		
		String placeName = dataArray[0];
	
		keyOut.set(Integer.parseInt(dataArray[1]));
		valueOut.set(placeName+"\t"+dataArray[2]);
		context.write(keyOut, valueOut);
		
	}

}

