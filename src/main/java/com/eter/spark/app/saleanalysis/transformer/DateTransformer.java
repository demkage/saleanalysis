package com.eter.spark.app.saleanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Created by rusifer on 5/20/17.
 */
public class DateTransformer extends Transformer {
    private static final long serialVersionUID = -1923213510417800809L;
    private String inputCol = "saledate";
    private String outputPrefix = "day";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {


        return dataset.withColumn(outputPrefix + "OfMonth",
                dayofmonth(dataset.col(inputCol)).cast(DataTypes.IntegerType))
                .withColumn(outputPrefix + "OfYear",
                        dayofyear(dataset.col(inputCol)).cast(DataTypes.IntegerType))
                .withColumn(outputPrefix + "OfWeek",
                        date_format(dataset.col(inputCol), "u").cast(DataTypes.IntegerType))
                .distinct();

    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        structType = structType.add(outputPrefix + "OfMonth", DataTypes.IntegerType);
        structType = structType.add(outputPrefix + "OfYear", DataTypes.IntegerType);
        structType = structType.add(outputPrefix + "OfWeek", DataTypes.IntegerType);
        return structType;
    }

    @Override
    public String uid() {
        return "" + serialVersionUID;
    }



    public String getInputCol() {
        return inputCol;
    }

    public void setInputCol(String inputCol) {
        this.inputCol = inputCol;
    }

    public String getOutputPrefix() {
        return outputPrefix;
    }

    public void setOutputPrefix(String outputPrefix) {
        this.outputPrefix = outputPrefix;
    }
}
