package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PhotoSortMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	private Text valueOut = new Text();
	private IntWritable keyOut = new IntWritable();
//		private Text valueOut = new Text(), keyOut =new Text();

	
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length > 2){ // a not complete record with all data
			 String placeName = dataArray[0];
			 keyOut.set(Integer.parseInt(dataArray[2]));
			 valueOut.set(placeName);
			 context.write(keyOut,valueOut);
		}else{
			return;
			}
				
		
	
		//keyOut.set(dataArray[2]);
	//	keyOut.set(Integer.parseInt(dataArray[2]));
		//valueOut.set(placeName);
		//context.write(keyOut,valueOut);
		
	}

}

