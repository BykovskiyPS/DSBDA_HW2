package bdtc.lab2;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.util.*;

/**
 * Эмулирует работу задач Compute Intensive (Вычислительо сложная задача)
 * и Data Intensive (Задач с большим количеством пересылок данных)
 */
@Slf4j
public class SparkRddApplication {
    /**
     * @param inputRdd - преобразованный в JavaRDD массив элеметов типа Element
     * Для каждого элемента массива подсчитывается его факториал
     * @return результат подсчета в формате JavaRDD
     * */
    public static JavaRDD<BigInteger> computeIntensive(JavaRDD<Element> inputRdd) {
        return inputRdd.map(s -> {
            BigInteger fct = new BigInteger("1");

            for(BigInteger i = new BigInteger("1");
                i.compareTo(s.value)<=0;
                i = i.add(BigInteger.ONE))
            {
                fct = fct.multiply(i);
            }
            return fct;
        });
    }

    /**
     * @param inputRdd - преобразованный в JavaRDD массив элеметов типа Element
     * Собирает элементы с одинаковым ключом (Element.index) и производит операцию
     * reduce суммирование значений
     * @return результат подсчета в формате JavaPairRDD
     * */
    public static JavaPairRDD<Integer,BigInteger> dataIntensive(JavaRDD<Element> inputRdd) {
        return inputRdd
                .mapToPair(r -> new Tuple2<>(r.index, r.value))
                .reduceByKey(BigInteger::add);
    }


    /**
     * @param args - args[0]: входной файл, args[1] - выходная папка
     */
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length < 2) {
            throw new RuntimeException("Usage: java -jar SparkSQLApplication.jar input.file outputDirectory");
        }

        log.info("Appliction started!");
        log.debug("Application started");

        SparkConf conf = new SparkConf().setAppName("SparkRDDApplication").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Чтение массивов данных из входного файла
        // Формирование массивов для compute intensive и data intensive задач
        List<Element> elements = new ArrayList<>();
        List<Element> forComputeIntensive = new ArrayList<>();
        Scanner scanner = new Scanner(new File(args[0]));
        boolean firstLine = true;
        while(scanner.hasNext()){
            List<String> splitted = Arrays.asList(scanner.nextLine().split(" "));
            for (Integer i = 0; i < splitted.size(); i++) {
                Element newEl = new Element(new BigInteger(splitted.get(i)), i);
                elements.add(newEl);
                if(firstLine) {
                    forComputeIntensive.add(newEl);
                }
            }
            firstLine = false;
        }

        log.info("===============COUNTING COMPUTE INTENSIVE================");
        long startComputeIntensive = System.currentTimeMillis();

        // Compute intensive task
        JavaRDD<Element> computeRDD = sc.parallelize(forComputeIntensive);
        JavaRDD<BigInteger> resultCompute = computeIntensive(computeRDD);

        long finishComputeIntensive = System.currentTimeMillis();
        long durationComputeIntensive = finishComputeIntensive - startComputeIntensive;
        log.info("[DURATION] " + durationComputeIntensive + " ms");

        log.info("===============COUNTING DATA INTENSIVE================");
        long startDataIntensive = System.currentTimeMillis();

        // Data intensive task
        JavaRDD<Element> dataRDD = sc.parallelize(elements);
        JavaPairRDD<Integer,BigInteger> resultData = dataIntensive(dataRDD);

        long finishDataIntensive = System.currentTimeMillis();
        long durationDataIntensive = finishDataIntensive - startDataIntensive;
        log.info("[DURATION] " + durationDataIntensive + " ms");

        JavaPairRDD<Integer,BigInteger> pairedCompute = resultCompute.mapToPair(s -> {return new Tuple2<>(-1,s);});
        log.info("============SAVING FILE TO " + args[1] + " directory============");
        pairedCompute.zip(resultData).saveAsTextFile(args[1]);
    }
}

