package join;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
/**
 *  * Partition basaed on Key, ignore the int
 *   * @author Ying Zhou
 *    *
 *     */
public class WordPartitioner extends Partitioner<Text, Text> {
@Override
	public int getPartition(Text key, Text value, int numPartition) {
		
		String key1 = key.toString();
		char c1 = key1.charAt(0);
		int put = (int)c1;
		if (put < 100) 
			return 0;
		if (put < 200 )
			return 1;
		else
			return 2;
	}	
}

