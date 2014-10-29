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
import org.apache.hadoop.mapred.* ;
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
import org.apache.hadoop.mapred.lib.ChainMapper ;

public class BranchAndBound {
    static double eps = 1e-7 ;
	
	public static class BBMapper1 extends Mapper <Object, Text, IntWritable, Text> {
		
		static int n = 0;
		static int c[];
		static int p[];
		static double value1[][];
        static double minValue1[] ;
		static double value2[][][][];
		static double allMin[][];
		static double rowMin[][][];
		static double deeMin[][][][];
		static List<String> valueList ;
        static final int maxListSize = 100000 ;
        static final double Infinity = 1e99 ;
        static double minGlobalUpperBound = Infinity ;
        
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			// handle exception ?????
			Path inputdata = DistributedCache.getLocalCacheFiles(conf)[0];
			Scanner input = new Scanner(new BufferedReader(new FileReader(inputdata.toString())));
			n = input.nextInt();
			c = new int[n];
			value1 = new double[n][];
            minValue1 = new double[n];
            for( int i = 0 ; i < n ; i++ ) {
                minValue1[i] = Infinity ;
            }
			for( int i = 0 ; i < n ; i++ ) {
				c[i] = input.nextInt() ;
				value1[i] = new double[c[i]];
				for( int j = 0 ; j < c[i] ; j++ ) {
					value1[i][j] = input.nextDouble();
                    minValue1[i] = Math.min(minValue1[i], value1[i][j]);
				}
			}
			value2 = new double[n][n][][] ;
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = i + 1 ; j < n ; j++ ) {
					value2[i][j] = new double[c[i]][c[j]] ;
					for( int x = 0 ; x < c[i] ; x++ ) {
						for( int y = 0 ; y < c[j] ; y++ ) {
							value2[i][j][x][y] = input.nextDouble() ;
						}
					}
				}
			}
			for( int i = 0 ; i < n ; i++ ) {
				for( int j = 0 ; j < i ; j++ ) {
					value2[i][j] = new double[c[i]][c[j]] ;
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
			allMin = new double[n][n];
			rowMin = new double[n][n][];
			for (int i = 0; i < n; i ++) {
				for (int j = 0; j < n; j ++)
				if (i != j) {
					rowMin[i][j] = new double[c[i]];
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
			deeMin = new double[n][][][];
			for (int i = 0; i < n; i ++) {
				deeMin[i] = new double[c[i]][c[i]][n];
				for (int x = 0; x < c[i]; x ++) {
					for (int y = 0; y < c[i]; y ++)
					if (x != y) {
						for (int j = 0; j < n; j ++)
						if (i != j) {
							deeMin[i][x][y][j] = Infinity;
							for (int k = 0; k < c[j]; k ++)
							if (value2[i][j][x][k] - value2[i][j][y][k] < deeMin[i][x][y][j]) {
								deeMin[i][x][y][j] = value2[i][j][x][k] - value2[i][j][y][k];
							}
						}
					}
				}
			}
            valueList = new ArrayList<String>();
			Counter cnt = context.getCounter("MyCounter", "nSetup");
			cnt.increment(1);
		}
		
		public static double getLowerValue(int i, int x, int j, int y) {
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
		
		public static boolean checkDEE(int i, int x, int b[]) {
			for (int y = 0; y < c[i]; y ++)
			if (x != y) {
				double sum = 0;
				for (int j = 0; j < n; j ++) {
					if (b[j] == -1) {
						sum += deeMin[i][x][y][j];
					} else {
						sum += value2[i][j][x][b[j]] - value2[i][j][y][b[j]];
					}
				}
				if (sum > eps) return true;
			}
			return false;
		}
		
		public static double calcLowerBound(double a[], int b[], int k, int o) {
			if (checkDEE(k, o, b)) {
				return Infinity;
			}
			double result = 0.0;
			b[k] = o;
			for (int i = 0; i < n; i ++)
			if (b[i] != -1) {
				result += value1[i][b[i]];
				for (int j = 0; j < i; j ++) {
                    result += getLowerValue(i, b[i], j, b[j]);
				//	result += value2[i][j][b[i]][b[j]];
				}
			} else {
				double minValue = Infinity;
				for (int v = 0; v < c[i]; v ++) {
					double sum = value1[i][v];
					for (int j = 0; j < i; j ++) {
						sum += getLowerValue(i, v, j, b[j]);
					}
					if (sum < minValue) {
						minValue = sum;
					}
				}
				result += minValue;
			}
			b[k] = -1;
			return result;
		}
		
		public static double getUpperValue(int i, int x, int j, int y) {
			if (x == -1) x = p[i];
			if (y == -1) y = p[j];
			return value2[i][j][x][y];
		}
		
		public static double calcUpperBound(double a[], int b[], int k, int o) {
			double result = 0;
			b[k] = o;
			int d[] = new int[n];/*
			for (int i = 0; i < n; i ++) {
				d[i] = b[i];
				if (d[i] == -1) {
					double minValue = 1e99;
					for (int v = 0; v < c[i]; v ++) {
						double sum = value1[i][v];
						for (int j = 0; j < i; j ++) {
							sum += value2[i][j][v][d[j]];
						}
						if (sum < minValue) {
							minValue = sum;
							d[i] = v;
						}
					}
				}
				result += value1[i][d[i]];
				for (int j = 0; j < i; j ++) {
					result += value2[i][j][d[i]][d[j]];
				}
			}*/
			for (int i = 0; i < n; i ++) {
				d[i] = (b[i] != -1) ? b[i] : (int)(Math.random() * c[i]);
			}
			double sum = 0;
			for (int i = 0; i < n; i ++) {
				sum += value1[i][d[i]];
				for (int j = 0; j < i; j ++) {
					sum += value2[i][j][d[i]][d[j]];
				}
			}
			while (true) {
				boolean changed = false;
				for (int i = 0; i < n; i ++)
				if (b[i] == -1) {
					sum -= value1[i][d[i]];
					for (int j = 0; j < n; j ++)
					if (i != j) {
						sum -= value2[i][j][d[i]][d[j]];
					}
					double min_sum = 1e99;
					int min_v = -1;
					for (int v = 0; v < c[i]; v ++) {
						double temp_sum = sum + value1[i][v];
						for (int j = 0; j < n; j ++)
						if (i != j) {
							temp_sum += value2[i][j][v][d[j]];
						}
						if (temp_sum + eps < min_sum) {
							min_sum = temp_sum;
							min_v = v;
						}
					}
					sum = min_sum;
					if (min_v != d[i]) {
						d[i] = min_v;
						changed = true;
					}
				}
				if (!changed) break;
			}
			b[k] = -1;
			return sum;
		}
		
		public static String getInfo(double a[], int b[]) {
			String str = "" + a[0] + " " + a[1] + " " + a[2];
			for (int i = 0; i < n; i ++) {
				str = str + " " + b[i];
			}
			return str;
		}
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if (itr.countTokens() != n + 3) return;
			double a[] = new double[3];
			int b[] = new int[n];
			for (int i = 0; i < 3; i ++) {
				a[i] = Double.parseDouble(itr.nextToken());
            }
			for (int i = 0; i < n; i ++) {
				b[i] = Integer.parseInt(itr.nextToken());
			}
			int k = -1;
			for (int i = 0; i < n; i ++)
			if ((b[i] == -1) && ((k == -1) || (c[i] < c[k]))) {
				k = i;
			}
			if (k == -1) return;
			double globalUpperBound = a[0];
			double lowerBound[] = new double[c[k]];
			double upperBound[] = new double[c[k]];
			for (int i = 0; i < c[k]; i ++) {
				lowerBound[i] = calcLowerBound(a, b, k, i);
				upperBound[i] = calcUpperBound(a, b, k, i);
				globalUpperBound = Math.min(globalUpperBound, upperBound[i]);
			}
            minGlobalUpperBound = Math.min( globalUpperBound , minGlobalUpperBound ) ;
			a[0] = minGlobalUpperBound;
			for (int i = 0; i < c[k]; i ++)
			if (lowerBound[i] < minGlobalUpperBound + eps) {
				b[k] = i;
				a[1] = lowerBound[i];
				a[2] = upperBound[i];
//				int strID = (int)cnt.getValue() / 10000;
//				context.write(new IntWritable(strID), new Text(strInfo));
				String strInfo = getInfo(a, b);
                if( valueList.size() == maxListSize ) {
                    Counter cnt = context.getCounter("MyCounter", "mapListNum");
                    cnt.increment(1);
                    outputList(context) ;
                    // valueList.clear() ;
                    //valueList = new ArrayList<String>() ;
                }
                valueList.add(strInfo) ;
				Counter cnt = context.getCounter("MyCounter", "lines");
				cnt.increment(1);
			}
		}
        
        public static void outputList(Context context) throws IOException, InterruptedException {
            for( String outputValue: valueList ) {
                StringTokenizer itr = new StringTokenizer(outputValue);
                double a[] = new double[3] ;
                for( int i = 0 ; i < 3 ; i++ ) {
                    a[i] = Double.parseDouble(itr.nextToken()) ;
                }
                double lowerBound = a[1] , upperBound = a[2] ;
                if( lowerBound < minGlobalUpperBound + eps || upperBound < minGlobalUpperBound + eps ) {
                    int outputKey = (int)(Math.random()*37) ;
                    context.write( new IntWritable(outputKey) , new Text(outputValue) ) ;
                }
            }
            valueList.clear();
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            outputList(context) ;
            Counter cnt = context.getCounter("MyCounter", "cleanup");
            cnt.increment(1);
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
		static final double Infinity = 1e99;
		static List<String> valueList = new ArrayList<String>() ;
        static final int maxListSize = 1000000 ;
        static double minGlobalUpperBound = Infinity ;
        
        public static void outputList(Context context) throws IOException, InterruptedException {
            for( String outputValue: valueList ) {
                StringTokenizer itr = new StringTokenizer(outputValue);
                double a[] = new double[3] ;
                for( int i = 0 ; i < 3 ; i++ ) {
                    a[i] = Double.parseDouble(itr.nextToken()) ;
                }
                double lowerBound = a[1] , upperBound = a[2] ;
                if( lowerBound < minGlobalUpperBound + eps || upperBound < minGlobalUpperBound + eps ) {
                    Counter cnt = context.getCounter("MyCounter", "nodes");
                    cnt.increment(1);
                    context.write( NullWritable.get() , new Text(outputValue) ) ;
                }
            }
            valueList.clear();
        }
        
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if( valueList.size() == maxListSize ) {
                    outputList(context) ;
                    //valueList.clear() ;
                    //valueList = new ArrayList<String>() ;
                    Counter cnt = context.getCounter("MyCounter", "reduceListNum");
                    cnt.increment(1);
                }
                valueList.add(value.toString()) ;
				StringTokenizer itr = new StringTokenizer(value.toString());
                double globalUpperBound = Double.parseDouble(itr.nextToken());
                minGlobalUpperBound = Math.min( minGlobalUpperBound , globalUpperBound ) ;
			}
            outputList(context) ;
	/*		for (Text value : valueList) {
				context.write(NullWritable.get(), new Text(value));
			}*/
/*			for (Text value : values) {
				StringTokenizer itr = new StringTokenizer(value.toString());
				int globalUpperBound = Integer.parseInt(itr.nextToken());
                minGlobalUpperBound = Math.min( minGlobalUpperBound , globalUpperBound ) ;
				if (globalUpperBound < minGlobalUpperBound) {
					minGlobalUpperBound = globalUpperBound;
				}
			}*/
/*			for (Text value : values) {
				StringTokenizer itr = new StringTokenizer(value.toString());
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
			}*/
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
		int n = 16 ;
		String[] inputargs = new GenericOptionsParser(
					new Configuration(), args).getRemainingArgs();
		if( inputargs.length != 3 ) {
			System.err.println("Usage: branchandbound <input> <output_dir> <n>") ;
			System.exit(2) ;
		}
		n = Integer.parseInt(inputargs[2]);
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
        JobConf conf = new JobConf(new Configuration(), BranchAndBound.class) ;
        conf.setJobName("chain") ;
        conf.setInputFormat(TextInputFormat.class) ;
        conf.setOutputFormat(TextOutputFormat.class) ;
        
        JobConf a = new JobConf( false ) ;
        ChainMapper.addMapper(conf, BBMapper1.class , Object.class , Text.class, IntWritable.class, Text.class , false , a ) ;
        
        for( int i = 1 ; i <= n ; i++ ) {
            String input = prev_output ;
            String output = inputargs[1] + "/iteration" + i ;
            Job job = getJob(input, output, i) ;
            job.waitForCompletion(true) ; // if failed ????
            prev_output = output;
		}
	}
}
