package algoproject4;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

public class FriendRec extends Configured implements Tool {
	public static String id = "9993";

	public static HashMap<String, String[]> map = new HashMap<String, String[]>();

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

		BufferedReader br = new BufferedReader(new FileReader(
				"soc-LiveJournal1Adj.txt"));
		String line;
		while ((line = br.readLine()) != null) {

			String[] temp = line.toString().split("\\t");
			String userID = temp[0];
			if (temp.length == 2) {
				String[] friends = temp[1].split(",");
				map.put(userID, friends);

			} else {
				String[] temp2 = new String[1];
				temp2[0] = "-1";
				map.put(userID, temp2);
			}

		}
		br.close();

		System.out.print(map.size());
		int res = ToolRunner.run(new Configuration(), new FriendRec(), args);
		br = new BufferedReader(new FileReader("output/part-r-00000"));
		ArrayList<Integer> recommendation=new ArrayList<Integer>();
		ArrayList<Integer> count=new ArrayList<Integer>();
		
		
		while ((line = br.readLine()) != null) {
			recommendation.add(Integer.valueOf(line.split("\t")[0]));
			count.add(Integer.valueOf(line.split("\t")[1]));
		}
		quicksort(count,recommendation,0,count.size()-1);
		ListIterator li=count.listIterator();
		ListIterator liR=recommendation.listIterator();
		int[] countArray=new int[count.size()];
		int[] recommendationArray=new int[recommendation.size()];
		for(int i=0;i<count.size();i++){
			Integer integer=(Integer) li.next();
			countArray[i]=integer.intValue();
			integer= (Integer) liR.next();
			recommendationArray[i]=integer.intValue();
		}
		int from=-1;int to=-1;
		for(int i=0;i<countArray.length-1;i++){
			if(from==-1&&countArray[i]==countArray[i+1]){
				from=i;
				to=i+1;
			}else if(from>-1&&countArray[i+1]==countArray[from]){
				to=i+1;
			}else if(from>-1){
				//sort from "from" to "to"
				Arrays.sort(recommendationArray, from, to+1);
				from=-1;
				to=-1;
			}
		}
		if(from>-1){
			Arrays.sort(recommendationArray, from, to+1);
		}
		
		String output="";
		output+=id;
		output+="\t";
		for(int i=0;i<10&&i<recommendationArray.length;i++){
			output+=recommendationArray[i];
			if(i<=9&&i<=recommendationArray.length-2){
				output+=",";
			}else{
				output+="\n";
			}
		}
		BufferedWriter bw = new BufferedWriter(new FileWriter("output/finaloutput.txt"));
		bw.write(output);
		System.out.println(output);
		bw.close();
		System.exit(res);
	}

	public static void quicksort(ArrayList<Integer> a, ArrayList<Integer> b,
			int l, int r) {

		if (l < r) {
			int i, j, y;
			int x;
			i = l;
			j = r;
			x = a.get(i);
			y = b.get(i);

			while (i < j) {
				while (i < j && a.get(j) < x)
					j--;
				if (i < j) {
					a.set(i++, a.get(j));
					b.set(i - 1, b.get(j));
				}
				while (i < j && a.get(i) > x)
					i++;
				if (i < j) {
					a.set(j--, a.get(i));
					b.set(j + 1, b.get(i));
				}

			}

			a.set(i, x);
			b.set(i, y);

			quicksort(a, b, l, i - 1); 
			quicksort(a, b, i + 1, r); 
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		// @SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "FriendRec");
		job.setJarByClass(FriendRec.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] temp = value.toString().split("\\t");
			String middle = temp[0];
			String[] friends = map.get(id);
			if (contains(friends, middle) && contains(map.get(middle), id)) {// hjbjhvutkjg
				String[] friendOfMiddle = map.get(middle);
				for (int j = 0; j < friendOfMiddle.length; j++) {
					String lastId = friendOfMiddle[j];
					String[] friendOfLast = map.get(lastId);
					if (!lastId.equals(id)&&contains(friendOfLast, middle)
							&& !contains(friendOfLast, id)
							&& !contains(friends, lastId)) {
						word.set(lastId);
						context.write(word, ONE);
					}
				}
			}

		}

		private static boolean contains(String[] array, String key) {
			for (int i = 0; i < array.length; i++) {
				if (array[i].equals(key)) {
					return true;
				}
			}
			return false;
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
