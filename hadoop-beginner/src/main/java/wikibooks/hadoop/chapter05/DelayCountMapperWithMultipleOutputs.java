package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import wikibooks.hadoop.common.AirlinePerformanceParser;


public class DelayCountMapperWithMultipleOutputs extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

		// 출발지연 데어터 출력
		if (parser.isDepartureDelayAvailable()) {
			if (parser.getDepartureDelayTime() > 0) {
				// 출력키 설정
				outputKey.set("D," + parser.getYear() + "," + parser.getMonth());
				// 출력 데이터 생성
				context.write(outputKey, outputValue);
			} else if (parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);

			} else if (parser.getDepartureDelayTime() < 0) {
				context.getCounter(DelayCounters.early_departure).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_departure).increment(1);
		}

		if (parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
				// 출력키 설정
				outputKey.set("A," + parser.getYear() + "," + parser.getMonth());
			   // 출력데이터생성
				context.write(outputKey, outputValue);

			} else if (parser.getArriveDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_arrival).increment(1);
			} else if (parser.getArriveDelayTime() < 0) {
				context.getCounter(DelayCounters.early_arrival).increment(1);
			}
		} else {
			context.getCounter(DelayCounters.not_available_arrival).increment(1);
		}
	}
}
