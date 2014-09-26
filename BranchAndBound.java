/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf ;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext ;
import org.apache.hadoop.mapreduce.Counter ;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BranchAndBound {
	
	public static class BBMapper1 extends Mapper <Object, Text, IntWritable, Text> {
		
		static int n = 0;
		static int c[];
		static int p[];
		static int value1[][];
		static int value2[][][][];
		static int allMin[][];
		static int rowMin[][][];
		List<String> valueList ;
        static final int maxListSize = 10000000 ;
        static int minGlobalUpperBound = 2000000000 ;
        
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration() ;
			// handle exception ?????
			Path inputdata = DistributedCache.getLocalCacheFiles(conf)[0] ;
			Scanner input = new Scanner(new BufferedReader(new FileReader(inputdata.toString()))) ;
			n = input.nextInt() ;
			c = new int[n] ;
			value1 = new int[n][] ;
			for( int i = 0 ; i < n ; i++ ) {
				c[i] = input.nextInt() ;
				value1[i] = new int[c[i]] ;
				for( int j = 0 ; j < c[i] ; j++ ) {
					value1[i][j] = input.nextInt() ;
				}
			}
			value2 = new int[n][n][][] ;
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = i + 1 ; j < n ; j++ ) {
					value2[i][j] = new int[c[i]][c[j]] ;
					for( int x = 0 ; x < c[i] ; x++ ) {
						for( int y = 0 ; y < c[j] ; y++ ) {
							value2[i][j][x][y] = input.nextInt() ;
						}
					}
				}
			}
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = 0 ; j < i ; j++ ) {
					value2[i][j] = new int[c[i]][c[j]] ;
					for( int x = 0 ; x < c[i] ; x++ ) {
						for( int y = 0 ; y < c[j] ; y++ ) {
							value2[i][j][x][y] = value2[j][i][y][x] ;
						}
					}
				}
			}
			input.close() ;
			p = new int[n];
			for (int i = 0; i < n; i ++) {
				p[i] = 0;
				for (int j = 0; j < c[i]; j ++)
				if (value1[i][j] < value1[i][p[i]]) {
					p[i] = j;
				}
			}
			allMin = new int[n][n];
			rowMin = new int[n][n][];
			for (int i = 0; i < n; i ++) {
				for (int j = 0; j < n; j ++)
				if (i != j) {
					rowMin[i][j] = new int[c[i]];
					for (int k = 0; k < c[i]; k ++) {
						rowMin[i][j][k] = value2[i][j][k][0];
						for (int l = 0; l < c[j]; l ++)
						if (value2[i][j][k][l] < rowMin[i][j][k]) {
							rowMin[i][j][k] = value2[i][j][k][l];
						}
						if (rowMin[i][j][k] < allMin[i][j]) {
							allMin[i][j] = rowMin[i][j][k];
						}
					}
				}
			}
            List<String> valueList = new ArrayList<String>();
			Counter cnt = context.getCounter("MyCounter", "nSetup");
			cnt.increment(1);
		}
		
		public static int getLowerValue(int i, int x, int j, int y) {
			if ((x == -1) && (y == -1)) {
				return allMin[i][j];
			} else if (x == -1) {
				return rowMin[j][i][y];
			} else if (y == -1) {
				return rowMin[i][j][x];
			} else {
				return value2[i][j][x][y];
			}
		}
		
		public static int calcLowerBound(int a[], int k, int o) {
		/*	int result = a[1];
			for (int i = 0; i < n; i ++)
			if (i != k) {
				result -= getLowerValue(i, a[i + 3], k, -1);
				result += getLowerValue(i, a[i + 3], k, o);
			}
			result -= value1[k][p[k]];
			result += value1[k][o];*/
			int result = 0;
			a[k + 3] = o;
			for (int i = 0; i < n; i ++)
			if (a[i + 3] != -1) {
				result += value1[i][a[i + 3]];
				for (int j = 0; j < i; j ++)
				if (a[j + 3] != -1) {
					result += value2[i][j][a[i + 3]][a[j + 3]];
				}
			} else {
				int minValue = 2000000000;
				for (int v = 0; v < c[i]; v ++) {
					int sum = value1[i][v];
					for (int j = 0; j < i; j ++) {
						sum += getLowerValue(i, v, j, a[j + 3]);
					}
					if (sum < minValue) {
						minValue = sum;
					}
				}
				result += minValue;
			}
			a[k + 3] = -1;
			return result;
		}
		
		public static int getUpperValue(int i, int x, int j, int y) {
			if (x == -1) x = p[i];
			if (y == -1) y = p[j];
			return value2[i][j][x][y];
		}
		
		public static int calcUpperBound(int a[], int k, int o) {
		/*	int result = a[2];
			for (int i = 0; i < n; i ++)
			if (i != k) {
				result -= getUpperValue(i, a[i + 3], k, -1);
				result += getUpperValue(i, a[i + 3], k, o);
			}
			result -= value1[k][p[k]];
			result += value1[k][o];*/
			int result = 0;
			a[k + 3] = o;
			int b[] = new int[n];
			for (int i = 0; i < n; i ++) {
				b[i] = a[i + 3];
				if (b[i] == -1) {
					int minValue = 2000000000;
					for (int v = 0; v < c[i]; v ++) {
						int sum = value1[i][v];
						for (int j = 0; j < i; j ++) {
							sum += value2[i][j][v][b[j]];
						}
						if (sum < minValue) {
							minValue = sum;
							b[i] = v;
						}
					}
				}
				result += value1[i][b[i]];
				for (int j = 0; j < i; j ++) {
					result += value2[i][j][b[i]][b[j]];
				}
			}
			a[k + 3] = -1;
			return result;
		}
		
		public static String getInfo(int a[]) {
			String str = "" + a[0];
			for (int i = 1; i < a.length; i ++) {
				str = str + " " + a[i];
			}
			return str;
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if (itr.countTokens() != n + 3) return;
			int a[] = new int[n + 3];
			for (int i = 0; i < n + 3; i ++) {
				a[i] = Integer.parseInt(itr.nextToken());
			}
			int k = -1;
			for (int i = 0; i < n; i ++)
			if ((a[i + 3] == -1) && ((k == -1) || (c[i] < c[k]))) {
				k = i;
			}
			if (k == -1) return;
			int globalUpperBound = a[0];
			int lowerBound[] = new int[c[k]];
			int upperBound[] = new int[c[k]];
			for (int i = 0; i < c[k]; i ++) {
				lowerBound[i] = calcLowerBound(a, k, i);
				upperBound[i] = calcUpperBound(a, k, i);
				globalUpperBound = Math.min(globalUpperBound, upperBound[i]);
			}
            minGlobalUpperBound = Math.min( globalUpperBound , minGlobalUpperBound ) ;
			a[0] = minGlobalUpperBound;
			for (int i = 0; i < c[k]; i ++)
			if (lowerBound[i] <= minGlobalUpperBound) {
				a[k + 3] = i;
				a[1] = lowerBound[i];
				a[2] = upperBound[i];
//				int strID = (int)cnt.getValue() / 10000;
//				context.write(new IntWritable(strID), new Text(strInfo));
				String strInfo = getInfo(a);
                if( valueList.size() == maxListSize ) {
                    Counter cnt = context.getCounter("MyCounter", "listNum");
                    int output_key = (int)cnt.getValue() ;
                    for( String value: valueList ) {
                        context.write( new IntWritable(output_key) , new Text(value) ) ;
                    }
                    valueList = new ArrayList<String>() ;
                }
                valueList.add(strInfo) ;
				Counter cnt = context.getCounter("MyCounter", "lines");
				cnt.increment(1);
			}
		}
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            
        }
	}
/*
	public static class BBMapper2 extends Mapper <Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Counter cnt = context.getCounter("MyCounter", "lines");
			int strID = (int)cnt.getValue() / 10000;
			context.write(new IntWritable(strID), value);
			cnt.increment(1);
		}
	}
*/
	public static class BBReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		static final int Infinity = 2000000000;
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> valueList = new ArrayList<String>();
			for (Text value : values) {
				valueList.add(value.toString());
			}
	/*		for (Text value : valueList) {
				context.write(NullWritable.get(), new Text(value));
			}*/
			int minGlobalUpperBound = Infinity;
			for (String value : valueList) {
				StringTokenizer itr = new StringTokenizer(value);
				int globalUpperBound = Integer.parseInt(itr.nextToken());
				if (globalUpperBound < minGlobalUpperBound) {
					minGlobalUpperBound = globalUpperBound;
				}
			}
			for (String value : valueList) {
				StringTokenizer itr = new StringTokenizer(value);
				int a[] = new int[itr.countTokens()];
				for (int i = 0; i < a.length; i ++) {
					a[i] = Integer.parseInt(itr.nextToken());
				}
				a[0] = minGlobalUpperBound;
				int lowerBound = a[1];
				int upperBound = a[2];
				if ((lowerBound < minGlobalUpperBound) || (upperBound == minGlobalUpperBound)) {
					String strInfo = BBMapper1.getInfo(a);
					context.write(NullWritable.get(), new Text(strInfo));
					Counter cnt = context.getCounter("MyCounter", "nodes");
					cnt.increment(1);
				}
			}
		}
	}
/*
	static Job getJob(String input, String output, int iteration, int subIteration) throws Exception{
		Configuration conf = new Configuration() ;
		DistributedCache.addCacheFile(new URI("/bb/data"), conf) ;
		Job ret = new Job(conf, "BranchAndBound_iteration" + iteration + "_" + subIteration ) ;
		ret.setJarByClass(BranchAndBound.class) ;
		if( subIteration == 0 ) {
			ret.setMapperClass(BBMapper1.class) ;
		} else {
			ret.setMapperClass(BBMapper2.class) ;
		}
		ret.setReducerClass(BBReducer.class) ;
		FileInputFormat.setInputPaths(ret, new Path(input)) ;
		FileOutputFormat.setOutputPath(ret, new Path(output)) ;
		ret.setOutputKeyClass(IntWritable.class);
		ret.setOutputValueClass(Text.class);
		return ret ;
	}
*/
	static Job getJob(String input, String output, int iteration ) throws Exception{
        Configuration conf = new Configuration() ;
		DistributedCache.addCacheFile(new URI("/bb/data"), conf) ;
		Job ret = new Job(conf, "BranchAndBound_iteration" + iteration ) ;
		ret.setJarByClass(BranchAndBound.class) ;
        ret.setMapperClass(BBMapper1.class) ;
		ret.setReducerClass(BBReducer.class) ;
		FileInputFormat.setInputPaths(ret, new Path(input)) ;
		FileOutputFormat.setOutputPath(ret, new Path(output)) ;
		ret.setOutputKeyClass(IntWritable.class);
		ret.setOutputValueClass(Text.class);
		return ret ;
	}

    
	public static void main(String[] args) throws Exception {
		/*Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: branchandbound <input> <output>");
			System.exit(2);
		}
		Job job = new Job(conf, "branch and bound");
		job.setJarByClass(BranchAndBound.class);
		job.setMapperClass(BBMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
		int n = 20 ;
		String[] inputargs = new GenericOptionsParser(
					new Configuration(), args).getRemainingArgs();
		if( inputargs.length != 2 ) {
			System.err.println("Usage: branchandbound <input> <output_dir>") ;
			System.exit(2) ;
		}
		String prev_output = inputargs[0] ;
/*		for( int i = 1 ; i <= n ; i++ ) {
			for( int j = 0 ; j < 2 ; j++ ) {
				String input = prev_output ;
				String output = inputargs[1] + "/iteration" + i + "_" + j ;
				Job job = getJob(input, output, i, j) ;
				job.waitForCompletion(true) ; // if failed ????
				prev_output = output;
			}
		}
*/
        for( int i = 1 ; i <= n ; i++ ) {
            String input = prev_output ;
            String output = inputargs[1] + "/iteration" + i ;
            Job job = getJob(input, output, i) ;
            job.waitForCompletion(true) ; // if failed ????
            prev_output = output;
		}
		
		int b[] = new int[3];
		b[0] = 1;
		b[1] = 2;
		b[2] = 3;
		String s = BBMapper1.getInfo(b);
		System.out.println(s);
		Text value = new Text(s);
		StringTokenizer itr = new StringTokenizer(value.toString());
		if (itr.countTokens() != 3) return;
		int a[] = new int[3];
		for (int i = 0; i < 3; i ++) {
			a[i] = Integer.parseInt(itr.nextToken());
			System.out.println(a[i]);
		}
	}
}
