package com.eter.spark.app.saleanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.countDistinct;

/**
 * Created by rusifer on 5/20/17.
 */
public class CustomerCounter extends Transformer {
    private static final long serialVersionUID = -5552695322832454170L;
    private String inputColDate = "saledate";
    private String inpulCol = "customerid";
    private String outputCol = "customers";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> countCustomers = dataset.toDF().groupBy(dataset.col(inputColDate))
                .agg(dataset.col(inputColDate), countDistinct(inpulCol).as(outputCol));

        countCustomers = dataset.join(countCustomers,
                dataset.col(inputColDate).equalTo(countCustomers.col(inputColDate)))
                .drop(countCustomers.col(inputColDate));
        return countCustomers;
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType.add(outputCol, DataTypes.IntegerType);
        return structType;
    }

    @Override
    public String uid() {
        return "" + serialVersionUID;
    }

    public String getInputColDate() {
        return inputColDate;
    }

    public void setInputColDate(String inputColDate) {
        this.inputColDate = inputColDate;
    }

    public String getInpulCol() {
        return inpulCol;
    }

    public void setInpulCol(String inpulCol) {
        this.inpulCol = inpulCol;
    }

    public String getOutputCol() {
        return outputCol;
    }

    public void setOutputCol(String outputCol) {
        this.outputCol = outputCol;
    }
}
