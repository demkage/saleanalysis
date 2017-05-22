package com.eter.spark.app.saleanalysis.transformer;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by rusifer on 5/20/17.
 */
public class IntegrateResult extends Transformer {
    private static final long serialVersionUID = 4565590133766811893L;
    private String colDate = "saledate";
    private String colSales = "sales";
    private String colCustomers = "customers";
    private String colPromo = "promo";

    @Override
    public Dataset<Row> transform(Dataset<?> dataset) {
        return dataset.select(dataset.col(colDate), dataset.col(colSales),
                dataset.col(colCustomers), dataset.col(colPromo)).distinct();
    }

    @Override
    public Transformer copy(ParamMap paramMap) {
        return this;
    }

    @Override
    public StructType transformSchema(StructType structType) {
        StructType resultSchema = DataTypes.createStructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("saledate", DataTypes.DateType, false, Metadata.empty()),
                new StructField("sales", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("customers", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("promo", DataTypes.BooleanType, false, Metadata.empty())
        });

        return resultSchema;
    }

    @Override
    public String uid() {
        return "" + serialVersionUID;
    }
}
