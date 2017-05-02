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

public class PhotoSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	Text result = new Text();
	int row = 0; 
	public void reduce(IntWritable key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
	if(row<50){	
		
		for (Text text: values){
			context.write(text, key);
		}
		row++;
		}else{
			return;
		}
	}
}
