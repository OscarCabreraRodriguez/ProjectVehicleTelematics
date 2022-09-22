package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AccidentReporter {
    public SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>
    getAccidentReporter(DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> data){
        return  data.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                //We assign timestamps to the data
                return element.f0 * 1000;
            }
        }).filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            //We filter the cars that are stopped, speed=0 and grouped with the following parameters Time,VID, XWay, Seg, Dir
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple_filter) throws Exception {
                return tuple_filter.f2==0;
            }
        }).keyBy(1,3,5,6,7).window(SlidingEventTimeWindows.of(Time.seconds(120),Time.seconds(30))).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, TimeWindow>() {
            @Override
            //We create a Sliding Window of 120 seconds with slide 30 and check if there has been 4 consecutive events
            public void apply(Tuple tuple, TimeWindow window,
                              Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input,
                              Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                List<Integer> times =new ArrayList<Integer>();
                int max_time=-1,min_time=-1;
                int cont=0;
                //int lastTime=input.iterator().next().getField(0);
                for(Tuple8 element :input){
                    cont++;
                    times.add((int)element.f0);
                }
                if(cont==4){
                    min_time= Collections.min(times);
                    max_time=Collections.max(times);
                    Tuple7 result=new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>
                            (min_time, max_time, tuple.getField(0), tuple.getField(1), tuple.getField(2),
                                    tuple.getField(3),tuple.getField(4));
                    out.collect(result);
                }
            }
        });

    }
}
