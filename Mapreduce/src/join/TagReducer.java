package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import java.util.ArrayList;  
import java.util.Arrays;  
import java.util.Collections;  
import java.util.Comparator;  
import java.util.List;  
import java.util.Map.Entry;  

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;

public class TagReducer extends Reducer<Text, Text, Text, Text> {  
	

	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		
		Map<String, Integer> tagFrequency = new HashMap<String,Integer>();
		
		for (Text text: values){
			String tag = text.toString();
			if (tagFrequency.containsKey(tag)){
				tagFrequency.put(tag, tagFrequency.get(tag) +1);
			}else{
				tagFrequency.put(tag, 1);
			}
		}
		
		
		
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(tagFrequency.entrySet());  
		 Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {  
              
            @Override  
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {  
               
                return o2.getValue().compareTo(o1.getValue());  
            }  
        });  
  
		
		StringBuffer strBuf = new StringBuffer();
		
		int n = 0;
		
		for (Map.Entry<String, Integer> mapping : list) {  
			if(n<10){
				strBuf.append( "("+mapping.getKey() + ":"+mapping.getValue()+")");
				n++;
			}else{
				break;
			}
		}  
		
		
		
		result.set(strBuf.toString());
		context.write(key, result);
		
		
		
	}
}
