package com.eter.spark.app.saleanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.count;

/**
 * Created by rusifer on 5/20/17.
 */
public class SaleCounter extends Transformer {
    private static final long serialVersionUID = 624708206320987996L;
    private String inputCol = "saledate";
    private String outputCol = "sales";

    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> countSales = dataset.groupBy(dataset.col(inputCol))
                .agg(dataset.col(inputCol), count(dataset.col(inputCol)).as(outputCol));

        countSales = dataset.join(countSales, countSales.col(inputCol).equalTo(dataset.col(inputCol)))
                .drop(countSales.col(inputCol));

        return countSales;

    }

    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    public StructType transformSchema(StructType structType) {
//        StructType resultSchema = DataTypes.createStructType(new StructField[] {
//                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("saledate", DataTypes.DateType, false, Metadata.empty()),
//                new StructField("sales", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("customers", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("promo", DataTypes.BooleanType, false, Metadata.empty())
//        });
        structType = structType.add(new StructField(outputCol, DataTypes.IntegerType, true, Metadata.empty()));
        return structType;
    }

    public String uid() {
        return "" + serialVersionUID;
    }

    public String getInputCol() {
        return inputCol;
    }

    public void setInputCol(String inputCol) {
        this.inputCol = inputCol;
    }

    public String getOutputCol() {
        return outputCol;
    }

    public void setOutputCol(String outputCol) {
        this.outputCol = outputCol;
    }
}
