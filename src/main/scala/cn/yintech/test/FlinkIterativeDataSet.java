package cn.yintech.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

public class FlinkIterativeDataSet {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final IterativeDataSet<Integer> initial = env.fromElements(0).iterate(10000);
        final MapOperator<Integer, Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();

                return i + ((x * x + y * y < 1) ? 1 : 0);
            }
        });

        final DataSet<Integer> count = initial.closeWith(iteration);
        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer c) throws Exception {
                return c / (double) 10000*4 ;
            }
        }).print();

        env.execute("Iterative Pi Example");


    }

}
