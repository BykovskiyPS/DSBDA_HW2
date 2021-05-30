package bdtc.lab2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkTest {

    SparkConf conf = new SparkConf()
            .setAppName("SparkRddApplication")
            .setMaster("local")
            .set("spark.driver.allowMultipleContexts", "true");
    JavaSparkContext sc = new JavaSparkContext(conf);

    @Test
    public void testComputeIntensive() {
        List<Element> elements = Arrays.asList(
                new Element(new BigInteger("10"), 0),
                new Element(new BigInteger("3"), 1),
                new Element(new BigInteger("5"), 2),
                new Element(new BigInteger("7"), 3),
                new Element(new BigInteger("11"), 4)
        );
        List<BigInteger> expected = Arrays.asList(
                new BigInteger("3628800"),
                new BigInteger("6"),
                new BigInteger("120"),
                new BigInteger("5040"),
                new BigInteger("39916800")
        );

        JavaRDD<Element> input = sc.parallelize(elements);
        JavaRDD<BigInteger> result = SparkRddApplication.computeIntensive(input);
        List<BigInteger> getted = result.collect();

        for (Integer i = 0; i < getted.size(); i++) {
            assert expected.get(i).equals(getted.get(i));
        }
    }

    @Test
    public void testDataIntensive() {
        List<Element> elements = Arrays.asList(
                new Element(new BigInteger("10"), 0),
                new Element(new BigInteger("3"), 1),
                new Element(new BigInteger("5"), 0),
                new Element(new BigInteger("7"), 1)
        );
        Map<Integer,BigInteger> expected = new HashMap<Integer,BigInteger>() {{
            put(0, new BigInteger("15"));
            put(1, new BigInteger("10"));
        }};

        JavaRDD<Element> input = sc.parallelize(elements);
        JavaPairRDD<Integer,BigInteger> result = SparkRddApplication.dataIntensive(input);
        Map<Integer,BigInteger> getted = result.collectAsMap();
        assert expected.equals(getted);
    }

}
