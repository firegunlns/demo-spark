package com.pingcap;

import static org.junit.Assert.assertTrue;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void test1(){
        String logFile = "/Users/alan/work/learn/java/demo-spark/data/lineitem.tbl"; // Should be some file on your system

        SparkConf conf = new SparkConf().setMaster("local").setAppName("test1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> rdd = sc.textFile(logFile);

        //long cnt = rdd.count();
        //JavaRDD<Double> countRdd = rdd.map(line -> Double.parseDouble(line.split("\\|")[4]));
        JavaRDD<Double> countRdd = rdd.map(new Function<String, Double>() {
            @Override
            public Double call(String line) throws Exception {
                String[] flds = line.split("\\|");
                Double col_4 = Double.parseDouble(flds[4]);
                return col_4;
            }
        });

        Double partCnt = countRdd.reduce(new Function2<Double,Double,Double>() {
            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.printf("part count is %f\n", partCnt);
        sc.close();
        sc.stop();

    }
}
