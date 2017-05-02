package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<IntWritable, Text, Text, Text> {
	Text result = new Text();
	Text k1 = new Text();
	 
	public void reduce(IntWritable key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		
	
		for (Text text: values){
			String[] dataArray = text.toString().split("\t");
			String t = dataArray[1];
			String k = dataArray[0] + "\t"+String.valueOf(key);
			result.set(t);
			k1.set(k);
			context.write(k1, result);
		}
	
		
	}
}
