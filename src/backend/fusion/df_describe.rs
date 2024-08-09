
use arrow::{
    array::{ArrayRef, RecordBatch, StringArray},
    compute::{cast, concat},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use datafusion::logical_expr::approx_percentile_cont;
use datafusion::{
    dataframe::DataFrame,
    logical_expr::{avg, case, col, count, is_null, lit, max, median, min, stddev, sum},
};
use std::sync::Arc; 
use datafusion::logical_expr::ExprSchemable;
use datafusion::logical_expr::JoinType;

#[allow(unused)]
pub struct DescribeDataFrame {
    df: DataFrame,
    functions: &'static [&'static str],
    schema: SchemaRef,
}

#[allow(unused)]
impl DescribeDataFrame {
    pub fn new(df: DataFrame) -> Self {
        let functions = &[
            "count",
            "null_count",
            "mean",
            "std",
            "min",
            "max",
            "median",
            "percentile25",
            "percentile50",
            "percentile75",
            "mode",
            "mode_count",
        ];

        let original_schema_fields = df.schema().fields().iter();

        //define describe column
        let mut describe_schemas = vec![Field::new("describe", DataType::Utf8, false)];
        describe_schemas.extend(original_schema_fields.clone().map(|field| {
            if field.data_type().is_numeric() {
                Field::new(field.name(), DataType::Float64, true)
            } else {
                Field::new(field.name(), DataType::Utf8, true)
            }
        }));
        Self {
            df,
            functions,
            schema: Arc::new(Schema::new(describe_schemas)),
        }
    }

    pub async fn to_record_batch(&self) -> anyhow::Result<RecordBatch> {
        let original_schema_fields = self.df.schema().fields().iter();

        let batches = vec![
            self.count(),
            self.null_count(),
            self.mean(),
            self.stddev(),
            self.min(),
            self.max(),
            self.percentile(25),
            self.percentile(50),
            self.percentile(75),
            self.medium(),
            self.count(),
            self.mode_count(),

        ];

        // first column with function names
        let mut describe_col_vec: Vec<ArrayRef> = vec![Arc::new(StringArray::from(
            self.functions
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>(),
        ))];
        for field in original_schema_fields {
            let mut array_data = vec![];
            for result in batches.iter() {
                let array_ref = match result {
                    Ok(df) => {
                        let batchs = df.clone().collect().await;
                        match batchs {
                            Ok(batchs)
                                if batchs.len() == 1
                                    && batchs[0].column_by_name(field.name()).is_some() =>
                            {
                                let column = batchs[0].column_by_name(field.name()).unwrap();
                                if field.data_type().is_numeric() {
                                    cast(column, &DataType::Float64)?
                                } else {
                                    cast(column, &DataType::Utf8)?
                                }
                            }
                            _ => Arc::new(StringArray::from(vec!["null"])),
                        }
                    }
                    //Handling error when only boolean/binary column, and in other cases
                    Err(err)
                        if err.to_string().contains(
                            "Error during planning: \
                                          Aggregate requires at least one grouping \
                                          or aggregate expression",
                        ) =>
                    {
                        Arc::new(StringArray::from(vec!["null"]))
                    }
                    Err(other_err) => {
                        panic!("{other_err}")
                    }
                };
                array_data.push(array_ref);
            }
            describe_col_vec.push(concat(
                array_data
                    .iter()
                    .map(|af| af.as_ref())
                    .collect::<Vec<_>>()
                    .as_slice(),
            )?);
        }

        let batch = RecordBatch::try_new(self.schema.clone(), describe_col_vec)?;

        Ok(batch)
    }

    fn count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| count(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn null_count(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .map(|f| {
                    sum(case(is_null(col(f.name())))
                        .when(lit(true), lit(1))
                        .otherwise(lit(0))
                        .unwrap())
                    .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn mean(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| avg(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn stddev(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| stddev(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn min(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| min(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn max(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| !matches!(f.data_type(), DataType::Binary | DataType::Boolean))
                .map(|f| max(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }

    fn medium(&self) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| median(col(f.name())).alias(f.name()))
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    fn percentile(&self, percentile: u8) -> anyhow::Result<DataFrame> {
        let original_schema_fields = self.df.schema().fields().iter();
        let ret = self.df.clone().aggregate(
            vec![],
            original_schema_fields
                .clone()
                .filter(|f| f.data_type().is_numeric())
                .map(|f| {
                    approx_percentile_cont(lit(percentile as f64 / 100.0), col(f.name()))
                        .alias(f.name())
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(ret)
    }
    

    fn mode(&self) -> anyhow::Result<DataFrame> {
        // Create a vector to hold the results for each column
        let fields = self.df.schema().fields().iter();
        let mut mode_dfs = Vec::new();
    
        for field in fields {
            let column_name = field.name();
            let col_expr = col(column_name);
    
            // Group by column value and count occurrences
            let count_expr = count(col_expr.clone()).alias("value_count");
            let group_expr = col_expr.clone().alias("value");
    
            // Create a subquery DataFrame to count occurrences
            let subquery = self.df.clone().aggregate(vec![group_expr.clone()], vec![count_expr.clone()])?;
    
            // Select the mode (value with the highest count)
            let max_count_expr = max(col("value_count")).alias("max_count");
            let mode_df = subquery.clone().aggregate(vec![], vec![max_count_expr])?;
            
            // Join the subquery on max_count to get the mode value
            let join_expr = col("value_count").eq(col("max_count"));
            let final_df = subquery
                .join(mode_df, JoinType::Inner, &["value_count"], &["max_count"], Some(join_expr))?
                .select(vec![col("value").alias(column_name)])?
                .limit(0, Some(1))?;
    
            // Cast the mode value to string type
            let casted_df = final_df.clone().with_column(
                column_name,
                col(column_name).cast_to(&DataType::Utf8, final_df.schema())?,
            )?;
    
            mode_dfs.push(casted_df);
        }
    
        // Combine all mode DataFrames into a single DataFrame
        let result_df = mode_dfs.into_iter().reduce(|df1, df2| {
            df1.join(df2, JoinType::Inner, &[], &[], None).unwrap()
        }).ok_or_else(|| anyhow::anyhow!("Failed to combine mode values"))?;
    
        Ok(result_df)
    }

    fn mode_count(&self) -> anyhow::Result<DataFrame> {
        let fields = self.df.schema().fields().iter();
    let mut mode_count_dfs = Vec::new();

    for field in fields {
        let column_name = field.name();
        let col_expr = col(column_name);

        // Group by column value and count occurrences
        let count_expr = count(col_expr.clone()).alias("value_count");
        let group_expr = col_expr.clone().alias("value");

        // Create a subquery DataFrame to count occurrences
        let subquery = self.df.clone().aggregate(vec![group_expr.clone()], vec![count_expr.clone()])?;

        // Select the mode count (highest count)
        let max_count_expr = max(col("value_count")).alias("max_count");
        let mode_count_df = subquery.clone().aggregate(vec![], vec![max_count_expr])?;
        
        // Cast the mode count value to string type
        let casted_df = mode_count_df.clone().with_column(
            "max_count",
            col("max_count").cast_to(&DataType::Utf8, mode_count_df.schema())?,
        )?.select(vec![col("max_count").alias(column_name)])?;

        mode_count_dfs.push(casted_df);
    }

    // Combine all mode count DataFrames into a single DataFrame
    let result_df = mode_count_dfs.into_iter().reduce(|df1, df2| {
        df1.join(df2, JoinType::Inner, &[], &[], None).unwrap()
    }).ok_or_else(|| anyhow::anyhow!("Failed to combine mode count values"))?;

    Ok(result_df)
}
}
    