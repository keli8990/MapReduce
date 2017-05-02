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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;

public class JoinAddReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		
		Map<String, Integer> ownerFrequency = new HashMap<String,Integer>();
		
		for (Text text: values){
			String ownerId = text.toString();
			if (ownerFrequency.containsKey(ownerId)){
				ownerFrequency.put(ownerId, ownerFrequency.get(ownerId) +1);
			}else{
				ownerFrequency.put(ownerId, 1);
			}
		}
		StringBuffer strBuf = new StringBuffer();
		int sum = 0;
		for (String ownerId: ownerFrequency.keySet()){
			sum += ownerFrequency.get(ownerId);
			//strBuf.append("\t"+sum);
		}
		result.set("\t"+sum);
		context.write(key, result);
	}
}
