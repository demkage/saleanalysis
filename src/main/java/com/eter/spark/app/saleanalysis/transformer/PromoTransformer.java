package com.eter.spark.app.saleanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.expr;

/**
 * Created by rusifer on 5/20/17.
 */
public class PromoTransformer extends Transformer {
    private static final long serialVersionUID = 5196125503212217762L;
    private String colDate = "saledate";
    private String inputCol = "promo";
    private String outputCol = "isPromo";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        Dataset<Row> promo = dataset.toDF().withColumn(inputCol,
                expr("case when " + inputCol + " > 1 then 1 else 0 end").as(outputCol)).distinct();

        return promo;
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

    public String getColDate() {
        return colDate;
    }

    public void setColDate(String colDate) {
        this.colDate = colDate;
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
