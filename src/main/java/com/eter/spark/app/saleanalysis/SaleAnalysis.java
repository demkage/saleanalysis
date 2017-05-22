package com.eter.spark.app.saleanalysis;

import com.eter.spark.app.saleanalysis.transformer.*;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by rusifer on 5/20/17.
 */
public class SaleAnalysis {
    private static final Logger log = LoggerFactory.getLogger(SaleAnalysis.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            log.error("Can't find argument for model output");
            log.debug("Actual arguments length: " + args.length);
            log.info("Use <application-name> path/to/model");
            return;
        }

        String output = args[0];

        log.info("Set model output as: " + output);

        SparkSession session = new SparkSession.Builder()
                .appName("SaleAnalysis")
                .config("spark.sql.hive.metastore.version", "3.0.0")
                .config("spark.sql.hive.metastore.jars", "/usr/local/hadoop/share/hadoop/yarn/*:" +
                        "/usr/local/hadoop/share/hadoop/yarn/lib/*:" +
                        "/usr/local/hadoop/share/mapreduce/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/mapreduce/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hadoop/share/hadoop/hdfs/*:" +
                        "/usr//local/hadoop/etc/hadoop:" +
                        "/usr/local/hadoop/share/hadoop/common/lib/*:" +
                        "/usr/local/hadoop/share/hadoop/common/*:" +
                        "/usr/local/hive/lib/*:")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = session.sql("SELECT id, saledate, productid, customerid, price, promo " +
                "FROM sales");

        Pipeline pipeline = buildDataTransformPipeline();

        PipelineModel pipelineModel = pipeline.fit(dataset);
        Dataset<Row> transformed = pipelineModel.transform(dataset);

        OneHotEncoder stateDayOfMonthEncoder = new OneHotEncoder()
                .setInputCol("dayOfMonth")
                .setOutputCol("dayOfMonthVec");
        OneHotEncoder stateDayOfWeekEncoder = new OneHotEncoder()
                .setInputCol("dayOfWeek")
                .setOutputCol("dayOfWeekVec");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[] {"dayOfMonthVec", "dayOfWeekVec", "promo"})
                .setOutputCol("features");

        LinearRegression lr = new LinearRegression()
                .setLabelCol("sales").setFeaturesCol("features");

        Pipeline linearRegressionPipeline = new Pipeline();
        linearRegressionPipeline.setStages(new PipelineStage[] { stateDayOfMonthEncoder, stateDayOfWeekEncoder,
                assembler, lr});

        PipelineModel linearRegressionModel = linearRegressionPipeline.fit(transformed);

        linearRegressionModel.transform(transformed).show();

        linearRegressionModel.save(output);

    }

    private static Pipeline buildDataTransformPipeline() {
        Pipeline pipeline = new Pipeline();

        SaleCounter saleCounter = new SaleCounter();
        CustomerCounter customerCounter = new CustomerCounter();
        PromoTransformer promoTransformer = new PromoTransformer();
        IntegrateResult integrateResult = new IntegrateResult();
        DateTransformer dateTransformer = new DateTransformer();

        pipeline.setStages(new PipelineStage[]{saleCounter, customerCounter, promoTransformer, integrateResult,
                dateTransformer});

        return pipeline;
    }
}
