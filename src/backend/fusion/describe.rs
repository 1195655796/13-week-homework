use core::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::approx_percentile_cont;
use datafusion::{
    dataframe::DataFrame,
    functions::expr_fn::length,
    functions_array::length::array_length,
    logical_expr::{avg, case, cast, col, count, is_null, lit, max, min, stddev, sum},
};
use datafusion::logical_expr::JoinType;
use datafusion::logical_expr::ExprSchemable;
use datafusion::functions::expr_fn::abs;
use futures::executor::block_on;
use arrow::array::StringArray;
use arrow::array::Float64Array;
use std::collections::HashMap;
use datafusion::logical_expr::Expr;
use arrow::array::Int64Array;
use std::ops::Sub;
use arrow::array::Array;
use crate::ReplDisplay;
use anyhow::anyhow;
use arrow::array::LargeStringArray;
use datafusion::logical_expr::median;
use datafusion::logical_expr::Expr::Literal;
use std::collections::HashSet;
#[allow(unused)]
#[derive(Debug)]
pub enum DescribeMethod {
    Total,
    NullTotal,
    Stddev,
    Min,
    Max,
    Mean,
    Median,
    Percentile(u8),
    Mode,
    ModeCount,
}

#[derive(Debug)]
pub struct DataFrameDescriber {
    original: DataFrame,
    transformed: DataFrame,
    methods: Vec<DescribeMethod>,
}

impl DataFrameDescriber {
    pub async fn try_new(df: DataFrame) -> anyhow::Result<Self> {
        let fields = df.schema().fields().iter();
        // change all temporal columns to Float64
        let expressions = fields
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), DataType::Float64),
                    dt if dt.is_numeric() => cast(col(field.name()), DataType::Float64),
                    DataType::List(_) | DataType::LargeList(_) => array_length(col(field.name())),
                    DataType::Utf8 | DataType::LargeUtf8 => col(field.name()),
                    _ => length(cast(col(field.name()), DataType::Utf8)),
                };
                expr.alias(field.name())
            })
            .collect();

        let transformed = df.clone().select(expressions)?;
        println!("transformed: {:?}", transformed);
        //println!("transformed:\n{}", DataFrame::display(transformed.clone()).await?);

        Ok(Self {
            original: df,
            transformed,
            methods: vec![
                DescribeMethod::Total,
                DescribeMethod::NullTotal,
                DescribeMethod::Stddev,
                DescribeMethod::Mean,
                DescribeMethod::Min,
                DescribeMethod::Max,

                DescribeMethod::Median,
                DescribeMethod::Mode,
                DescribeMethod::ModeCount,
                DescribeMethod::Percentile(25),
                DescribeMethod::Percentile(50),
                DescribeMethod::Percentile(75),
                // 作业：实现 25th, 50th, 75th percentile
               
    

            ],
        })
    }

    pub async fn describe(&self) -> anyhow::Result<DataFrame> {
        let df = self.do_describe().await?;
        self.cast_back(df)
    }
    pub async fn do_describe(&self) -> anyhow::Result<DataFrame> {
        let mut df_vec: Vec<DataFrame> = Vec::new();
    
        for method in &self.methods {
            let df = self.transformed.clone();
            let stat_df = match method {
                DescribeMethod::Total => total(df).unwrap(),
                DescribeMethod::NullTotal => null_total(df).unwrap(),
                DescribeMethod::Stddev => std_div(df).unwrap(),
                DescribeMethod::Min => minimum(df).unwrap(),
                DescribeMethod::Max => maximum(df).unwrap(),
                DescribeMethod::Mean => mean(df).unwrap(),
                DescribeMethod::Median => med(df).unwrap(),
                DescribeMethod::Percentile(p) => percentile(df, *p as f64 / 100.0).unwrap(),
                DescribeMethod::Mode => mode(df).unwrap(),
                DescribeMethod::ModeCount => mode_count(df).unwrap(),
            };
    
            // add a new column to the beginning of the DataFrame
            let mut select_expr = vec![lit(method.to_string()).alias("describe")];
            println!("select_expr: {:?}", select_expr);
            select_expr.extend(stat_df.schema().fields().iter().map(|f| col(f.name()).alias(f.name())));
    
            let stat_df = stat_df.select(select_expr).unwrap();
            println!("stat_df:\n{}", DataFrame::display(stat_df.clone()).await?);
            df_vec.push(stat_df.clone());
        }
        Ok(df_vec.into_iter().reduce(|df1, df2| df1.union(df2).unwrap()).unwrap())
    }

    pub fn cast_back(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        // we need the describe column
        let describe = Arc::new(Field::new("describe", DataType::Utf8, false));
        let mut fields = vec![&describe];
        fields.extend(self.original.schema().fields().iter());
        let expressions = fields
            .into_iter()
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        cast(col(field.name()), DataType::Int32)
                    }
                    DataType::Utf8 | DataType::LargeUtf8 => col(field.name()),
                    _ => col(field.name()),
                };
                expr.alias(field.name())
            })
            .collect();

        Ok(df
            .select(expressions)?
            .sort(vec![col("describe").sort(true, false)])?)
    }
}

impl fmt::Display for DescribeMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DescribeMethod::Total => write!(f, "total"),
            DescribeMethod::NullTotal => write!(f, "null_total"),
            DescribeMethod::Stddev => write!(f, "stddev"),
            DescribeMethod::Min => write!(f, "min"),
            DescribeMethod::Max => write!(f, "max"),
            DescribeMethod::Mean => write!(f,"mean"),
            DescribeMethod::Median => write!(f, "median"),
            DescribeMethod::Percentile(p) => write!(f, "percentile_{}", p),
            DescribeMethod::Mode => write!(f, "mode"),
            DescribeMethod::ModeCount => write!(f, "mode_count"),


        }
    }
}



fn total(df: DataFrame) -> anyhow::Result<DataFrame> {
    let original_schema_fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        original_schema_fields
            .clone()
            .map(|f| count(col(f.name())).alias(f.name()))
            .collect::<Vec<_>>(),
    )?;
    Ok(ret)
}

fn null_total(df: DataFrame) -> anyhow::Result<DataFrame> {
    let fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        fields
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


fn mean(df: DataFrame) -> anyhow::Result<DataFrame> {
    let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();

    let mut numerical_columns = vec![];
    let mut string_columns = vec![];

    for field in &original_schema_fields {
        match field.data_type() {
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
                numerical_columns.push(field.name().clone());
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                string_columns.push(field.name().clone());
            }
            _ => {}
        }
    }

    // Calculate mean for numerical columns
    let mut exprs = numerical_columns
        .iter()
        .map(|col_name| avg(col(col_name)).alias(col_name))
        .collect::<Vec<Expr>>();

    // Calculate mean for string columns separately
    let mut string_means = vec![];
    for col_name in &string_columns {
        let mean_string_value = calculate_mean_string(df.clone(), col(col_name.clone()))?;
        string_means.push((mean_string_value, col_name.clone()));
    }

    let mut ret = df.clone().aggregate(vec![], exprs)?;
    
    // Add string means to the result
    for (mean_value, col_name) in string_means {
        ret = ret.with_column(&col_name.clone().to_string(), Literal(mean_value.into()))?;
    }

    // Reorder the columns to match the original order
    let reordered_exprs = original_schema_fields
        .iter()
        .map(|field| col(field.name()))
        .collect::<Vec<Expr>>();

    let ret = ret.select(reordered_exprs)?;

    println!("mean ret : {:?}", ret);
    Ok(ret)
}

fn calculate_mean_string(df: DataFrame, expr: Expr) -> anyhow::Result<String> {
    let len_expr = length(expr.clone()).alias("len");
    let df_with_length = df.with_column("len", len_expr.clone())?;

    // Calculate the mean length of the strings
    let mean_length_df = df_with_length.clone().aggregate(vec![], vec![avg(col("len")).alias("mean_len")])?;
    let mean_len_value = block_on(mean_length_df.select(vec![col("mean_len")])?.collect())?
        .first()
        .ok_or_else(|| anyhow!("Failed to get mean length"))?
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| anyhow!("Failed to downcast mean length to Float64Array"))?
        .value(0);

    // Find the string closest to the mean length
    let df_sorted_by_length = df_with_length.sort(vec![col("len").sort(true, true)])?;
    let closest_string_row = df_sorted_by_length
        .limit(0, None)?
        .filter(col("len").eq(lit(mean_len_value.round() as i64)))?
        .limit(0, Some(1))?;

    let column_name = match expr {
        Expr::Column(ref column) => column.name.clone(),
        _ => return Err(anyhow!("Expected a column expression")),
    };

    let collected_result = block_on(closest_string_row.select(vec![col(&column_name)])?.collect())?;
    let batch = collected_result
        .first()
        .ok_or_else(|| anyhow!("Failed to get mean row"))?;

    let column = batch.column(0);
    let array = column
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
        .ok_or_else(|| anyhow!("Failed to downcast column to LargeStringArray: {:?}", column.data_type()))?;

    let mean_string_value = array.value(0).to_string();

    Ok(mean_string_value)
}




fn med(df: DataFrame) -> anyhow::Result<DataFrame> {
    let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();

    let mut numerical_columns = vec![];
    let mut string_columns = vec![];

    for field in &original_schema_fields {
        match field.data_type() {
            DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
                numerical_columns.push(field.name().clone());
            }
            DataType::Utf8 | DataType::LargeUtf8 => {
                string_columns.push(field.name().clone());
            }
            _ => {}
        }
    }

    // Calculate median for numerical columns
    let mut exprs = numerical_columns
        .iter()
        .map(|col_name| median(col(col_name)).alias(col_name))
        .collect::<Vec<Expr>>();

    // Calculate median for string columns separately
    let mut string_medians = vec![];
    for col_name in &string_columns {
        let median_string_value = calculate_median_string(df.clone(), col(col_name.clone()))?;
        string_medians.push((median_string_value, col_name.clone()));
    }

    let mut ret = df.clone().aggregate(vec![], exprs)?;
    
    // Add string medians to the result
    for (median_value, col_name) in string_medians {
        ret = ret.with_column(&col_name.clone().to_string(), Literal(median_value.into()))?;
    }
    let reordered_exprs = original_schema_fields
    .iter()
    .map(|field| col(field.name()))
    .collect::<Vec<Expr>>();

let ret = ret.select(reordered_exprs)?;

    println!("median ret : {:?}", ret);
    Ok(ret)
}
fn calculate_median_string(df: DataFrame, expr: Expr) -> anyhow::Result<String> {
    let len_expr = length(expr.clone()).alias("len");
    let df_with_length = df.with_column("len", len_expr.clone())?;
    let df_sorted = df_with_length.sort(vec![col("len").sort(true, true)])?;

    let total_rows = block_on(df_sorted.clone().count())?;
    println!("Total rows: {}", total_rows);

    let median_index = total_rows / 2;
    let df_limited = df_sorted.limit(0, Some(median_index + 1))?;
    let median_row = df_limited.limit(median_index, Some(1))?;

    let column_name = match expr {
        Expr::Column(ref column) => column.name.clone(),
        _ => return Err(anyhow!("Expected a column expression")),
    };

    let collected_result = block_on(median_row.select(vec![col(&column_name)])?.collect())?;
    println!("Collected result: {:?}", collected_result);

    let batch = collected_result
        .first()
        .ok_or_else(|| anyhow!("Failed to get median row"))?;

    let column = batch.column(0);
    println!("Column type: {:?}", column.data_type());

    let array = column
        .as_any()
        .downcast_ref::<arrow::array::LargeStringArray>()
        .ok_or_else(|| anyhow!("Failed to downcast column to LargeStringArray: {:?}", column.data_type()))?;

    let median_string_value = array.value(0).to_string();

    Ok(median_string_value)
}
    fn minimum(df: DataFrame) -> anyhow::Result<DataFrame> {
        // Clone the original schema fields
        let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();
    
        // Separate numerical and string columns
        let mut numerical_columns = vec![];
        let mut string_columns = vec![];
    
        for field in &original_schema_fields {
            match field.data_type() {
                DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
                    numerical_columns.push(field.name().clone());
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    string_columns.push(field.name().clone());
                }
                _ => {}
            }
        }
    
        // Aggregate numerical columns to get min values
        let mut exprs = numerical_columns
            .iter()
            .map(|col_name| min(col(col_name)).alias(&format!("{}", col_name)))
            .collect::<Vec<Expr>>();
    
        // Aggregate string columns to get the min values
        let string_exprs = string_columns
            .iter()
            .map(|col_name| min(col(col_name)).alias(&format!("{}", col_name)))
            .collect::<Vec<Expr>>();
    
        // Combine numeric min expressions and string min expressions
        let final_exprs = [exprs, string_exprs].concat();
    
        // Perform the final aggregation
        let ret = df.clone().aggregate(vec![], final_exprs)?;
        let reordered_exprs = original_schema_fields
    .iter()
    .map(|field| col(field.name()))
    .collect::<Vec<Expr>>();

let ret = ret.select(reordered_exprs)?;
        println!("minimum ret : {:?}", ret);
    
        Ok(ret)
    }

    fn maximum(df: DataFrame) -> anyhow::Result<DataFrame> {
        // Clone the original schema fields
        let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();
    
        // Separate numerical and string columns
        let mut numerical_columns = vec![];
        let mut string_columns = vec![];
    
        for field in &original_schema_fields {
            match field.data_type() {
                DataType::Int64 | DataType::UInt64 | DataType::Float64=> {
                    numerical_columns.push(field.name().clone());
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    string_columns.push(field.name().clone());
                }
                _ => {}
            }
        }
    
        // Aggregate numerical columns to get max values
        let mut exprs = numerical_columns
            .iter()
            .map(|col_name| max(col(col_name)).alias(&format!("{}", col_name)))
            .collect::<Vec<Expr>>();
    
        // Aggregate string columns to get the max values
        let string_exprs = string_columns
            .iter()
            .map(|col_name| max(col(col_name)).alias(&format!("{}", col_name)))
            .collect::<Vec<Expr>>();
    
        // Combine numeric max expressions and string max expressions
        let final_exprs = [exprs, string_exprs].concat();
    
        // Perform the final aggregation
        let ret = df.clone().aggregate(vec![], final_exprs)?;
         // Reorder the columns in the resulting DataFrame to match the original order
    let reordered_exprs = original_schema_fields
    .iter()
    .map(|field| col(field.name()))
    .collect::<Vec<Expr>>();

let ret = ret.select(reordered_exprs)?;
        println!("maximum ret : {:?}", ret);
    
        Ok(ret)
    }
fn std_div(df: DataFrame) -> anyhow::Result<DataFrame> {

    let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();

        let mut exprs = vec![];

        for field in &original_schema_fields {
            match field.data_type() {
                DataType::Int64 | DataType::UInt64 | DataType::Float64=> {
                    exprs.push(stddev(col(field.name())).alias(field.name()));
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    let length_col = length(col(field.name())).alias(&format!("{}", field.name()));
                    exprs.push(stddev(length_col).alias(&format!("{}", field.name())));
                }
                _ => {}
            }
        }

        let ret = df.clone().aggregate(vec![], exprs)?;

        Ok(ret)
    }

    fn percentile(df: DataFrame, q: f64) -> anyhow::Result<DataFrame> {
        let original_schema_fields = df.schema().fields().iter().cloned().collect::<Vec<_>>();
    
        let mut numerical_columns = vec![];
        let mut string_columns = vec![];
    
        for field in &original_schema_fields {
            match field.data_type() {
                DataType::Int64 | DataType::UInt64 | DataType::Float64 => {
                    numerical_columns.push(field.name().clone());
                }
                DataType::Utf8 | DataType::LargeUtf8 => {
                    string_columns.push(field.name().clone());
                }
                _ => {}
            }
        }
    
        // Calculate percentile for numerical columns
        let mut exprs = numerical_columns
    .iter()
    .enumerate()
    .map(|(i, col_name)| {
        approx_percentile_cont(col(col_name), lit(q)).alias(&format!("{}", col(col_name)))
    })
    .collect::<Vec<Expr>>();
    
        // Calculate percentile for string columns
        let mut string_percentiles = vec![];
        for col_name in &string_columns {
            let percentile_value = calculate_percentile_string(df.clone(), col(col_name.clone()), q)?;
            string_percentiles.push((percentile_value, col_name.clone()));
        }
    
        let mut ret = df.clone().aggregate(vec![], exprs)?;
    
        // Add string percentiles to the result
        for (percentile_value, col_name) in string_percentiles {
            ret = ret.with_column(&col_name.clone().to_string(), Literal(percentile_value.into()))?;
        }
        let reordered_exprs = original_schema_fields
    .iter()
    .map(|field| col(field.name()))
    .collect::<Vec<Expr>>();

let ret = ret.select(reordered_exprs)?;
    
        println!("percentile ret : {:?}", ret);
        Ok(ret)
    }
    
    fn calculate_percentile_string(df: DataFrame, expr: Expr, q: f64) -> anyhow::Result<String> {
        let len_expr = length(expr.clone()).alias("len");
        let df_with_length = df.with_column("len", len_expr.clone())?;
        let df_sorted = df_with_length.sort(vec![col("len").sort(true, true)])?;
    
        let total_rows = block_on(df_sorted.clone().count())?;
        println!("Total rows: {}", total_rows);
    
        let percentile_index = if total_rows == 0 {
            0
        } else {
            let index = (total_rows as f64 * q).floor() as usize;
            std::cmp::min(index, total_rows - 1)
        };
        let df_limited = df_sorted.limit(0, Some(percentile_index + 1))?;
        let percentile_row = df_limited.limit(percentile_index, Some(1))?;
    
        let column_name = match expr {
            Expr::Column(ref column) => column.name.clone(),
            _ => return Err(anyhow!("Expected a column expression")),
        };
    
        let collected_result = block_on(percentile_row.select(vec![col(&column_name)])?.collect())?;
        println!("Collected result: {:?}", collected_result);
    
        let batch = collected_result
            .first()
            .ok_or_else(|| anyhow!("Failed to get percentile row"))?;
    
        let column = batch.column(0);
        println!("Column type: {:?}", column.data_type());
    
        let array = column
            .as_any()
            .downcast_ref::<arrow::array::LargeStringArray>()
            .ok_or_else(|| anyhow!("Failed to downcast column to LargeStringArray: {:?}", column.data_type()))?;

    let percentile_string_value = array.value(0).to_string();

    Ok(percentile_string_value)
}


    fn mode(df: DataFrame) -> anyhow::Result<DataFrame> {
        let fields = df.schema().fields().iter();
        let mut mode_dfs = Vec::new();
    
        for field in fields {
            let column_name = field.name();
            let col_expr = col(column_name);
    
            // Group by column value and count occurrences
            let count_expr = count(col_expr.clone()).alias("value_count");
            let group_expr = col_expr.clone().alias("value");
    
            // Create a subquery DataFrame to count occurrences
            let subquery = df.clone().aggregate(vec![group_expr.clone()], vec![count_expr.clone()])?;
    
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



fn mode_count(df: DataFrame) -> anyhow::Result<DataFrame> {
    let fields = df.schema().fields().iter();
    let mut mode_count_dfs = Vec::new();

    for field in fields {
        let column_name = field.name();
        let col_expr = col(column_name);

        // Group by column value and count occurrences
        let count_expr = count(col_expr.clone()).alias("value_count");
        let group_expr = col_expr.clone().alias("value");

        // Create a subquery DataFrame to count occurrences
        let subquery = df.clone().aggregate(vec![group_expr.clone()], vec![count_expr.clone()])?;

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



#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::*;
    use std::sync::Arc;

    fn create_test_dataframe() -> DataFrame {
        // Create a simple RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("float_col", DataType::Float64, false),
            Field::new("int_col", DataType::Int32, false),
            Field::new("string_col", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as ArrayRef,
                Arc::new(Int32Array::from(vec![4, 5, 6, 7])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef,
            ],
        )
        .unwrap();

        // Convert the RecordBatch to a DataFrame
        let ctx = SessionContext::new();
        ctx.read_batch(batch).unwrap()
    }

    #[tokio::test]
    async fn test_try_new() {
        let df = create_test_dataframe();
        let describer = DataFrameDescriber::try_new(df);
        assert!(describer.is_ok());
    }

    #[tokio::test]
    async fn test_describe_total() {
        let df = create_test_dataframe();
        let describer = DataFrameDescriber::try_new(df).unwrap();

        let described_df = describer.describe().await;
        assert!(described_df.is_ok());

        let result = described_df.unwrap().collect().await.unwrap();
        assert_eq!(result.len(), 1); // should have one row for each statistic
        assert_eq!(result[0].num_columns(), 4); // describe + three columns
    }
    #[tokio::test]
    async fn test_percentile() {
        let df = create_test_dataframe();
        let percentile_df = percentile(df.clone(), 0.5).unwrap();
        let result = percentile_df.collect().await.unwrap();
        // Verify the percentile values
        assert_eq!(result.len(), 1); // should have one row for the percentile
        assert_eq!(result[0].num_columns(), 2); // percentile + one column

        let float_col = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(float_col.value(0), 2.5); // median of [1.0, 2.0, 3.0, 4.0] is 2.5

        let int_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(int_col.value(0), 5); // median of [4, 5, 6, 7] is 5.5 but approx_percentile_cont rounds down to 5
    }
}


