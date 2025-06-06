import logging
from logger import configure_logging
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    udf,
    percent_rank,
    stddev,
    count,
    avg as spark_avg,
    dense_rank,
    round as spark_round,
    percentile_approx,
)
from pyspark.sql.types import StringType, BooleanType, DoubleType
import pandas as pd
from pyspark.sql.functions import when, col, isnan, lit
from pyspark.sql.types import FloatType, DoubleType, StringType

configure_logging()
logger = logging.getLogger(__name__)


# fact_population
def apply_transformations_into_fact_population(
    df: pd.DataFrame,
) -> DataFrame:

    def categorize_age(age: int) -> str:
        if age is None:
            return "unknown"
        if age < 16:
            return "child"
        elif age <= 17:
            return "adolescent"
        elif age <= 35:
            return "young adult"
        elif age <= 65:
            return "adult"
        else:
            return "senior"

    def categorize_income_bracket(income: float) -> str:
        if income is None:
            return "unknown"
        if income < 30000:
            return "low"
        elif income < 75000:
            return "mid"
        else:
            return "high"

    def categorize_income_category(income: float) -> str:
        if income is None:
            return "unknown"
        if income < 30000:
            return "very low"
        elif income < 50000:
            return "low"
        elif income < 100000:
            return "medium"
        else:
            return "high"

    def unpaid_worker(
        employment: str, hrs_work: float, income: float
    ) -> bool:
        return employment == "employed" and hrs_work > 0 and income == 0
    

    def final_clean_nan(sdf):
        for field in sdf.schema.fields:
            c = field.name
            dtype = field.dataType

            if isinstance(dtype, (FloatType, DoubleType)):
                # For float/double columns → replace NaN with NULL
                sdf = sdf.withColumn(c, when(isnan(col(c)), None).otherwise(col(c)))

            elif isinstance(dtype, StringType):
                # For string columns → replace literal "NaN" with NULL
                sdf = sdf.withColumn(c, when(col(c) == lit("NaN"), None).otherwise(col(c)))

            # other column types → leave unchanged
        return sdf

    age_udf = udf(categorize_age, StringType())
    income_bracket_udf = udf(categorize_income_bracket, StringType())
    income_category_udf = udf(categorize_income_category, StringType())
    unpaid_udf = udf(unpaid_worker, BooleanType())

    spark = SparkSession.builder.config(
        "spark.jars", "jars/postgresql-42.7.3.jar"
    ).getOrCreate()
    sdf = spark.createDataFrame(df)

    sdf = sdf.withColumn("age_group", age_udf(col("age")))
    sdf = sdf.withColumn(
        "income_bracket", income_bracket_udf(col("income"))
    )
    sdf = sdf.withColumn(
        "income_category", income_category_udf(col("income"))
    )
    sdf = sdf.withColumn(
        "is_unpaid_worker",
        unpaid_udf(col("employment"), col("hrs_work"), col("income")),
    )
    sdf = final_clean_nan(sdf)
    return sdf


# This function derives from acs_cleaned
# fact_paid_workers
def generate_fact_paid_workers_table(
    sdf: DataFrame,
) -> DataFrame:
    window_spec = Window.partitionBy("age_group").orderBy("income")
    window_spec_desc = Window.partitionBy("age_group").orderBy(
        col("income").desc()
    )

    paid_workers_df = sdf.filter(
        (col("employment") == "employed") & (~col("is_unpaid_worker"))
    )

    paid_workers_df = (
        paid_workers_df.withColumn(
            "income_percentile_within_age_group",
            spark_round(percent_rank().over(window_spec), 3),
        )
        .withColumn(
            "income_rank_within_age_group",
            dense_rank().over(window_spec_desc),
        )
        .withColumn(
            "avg_income_by_age_group",
            spark_round(
                spark_avg("income").over(
                    Window.partitionBy("age_group")
                ),
                1,
            ),
        )
    )

    paid_workers_df = paid_workers_df.drop(
        "employment", "is_unpaid_worker"
    )

    return paid_workers_df


# This data mart derives from fact_paid_workers
def generate_data_mart_income_above_median_by_edu(
    sdf: DataFrame,
) -> DataFrame:
    median_income_row = sdf.select(
        percentile_approx("income", 0.5).alias("median_income")
    ).collect()[0]
    median_income = median_income_row["median_income"]

    sdf_filtered = sdf.filter(col("income") > median_income)

    result_df = (
        sdf_filtered.groupBy("edu")
        .agg(
            count("*").alias("count_paid_workers"),
            spark_round(spark_avg("income"), 1).alias("avg_income"),
        )
        .orderBy(col("avg_income").desc())
    )

    return result_df


# This data mart derives from fact_paid_workers
def generate_data_mart_work_summary_by_gender(
    sdf: DataFrame,
) -> DataFrame:
    return sdf.groupBy("gender").agg(
        spark_round(spark_avg("hrs_work"), 1).alias("avg_hrs_work"),
        spark_round(spark_avg("time_to_work"), 1).alias("avg_commute"),
    )


# This data mart derives from fact_paid_workers
def generate_data_mart_population_summary_by_age_group(
    sdf: DataFrame,
) -> DataFrame:
    return sdf.groupBy("age_group").agg(
        count("*").alias("population"),
        spark_round(stddev("income"), 1).alias("income_stddev"),
    )


# This data mart derives from fact_paid_workers
def generate_data_mart_income_summary_by_demo(
    sdf: DataFrame,
) -> DataFrame:
    return sdf.groupBy("age_group", "gender", "race").agg(
        spark_round(spark_avg("income"), 1).alias("avg_income")
    )


# This data mart derives from fact_population
def generate_data_mart_children_percentage_by_race_gender(
    sdf: DataFrame,
) -> DataFrame:
    total_count_df = sdf.groupBy("race", "gender").agg(
        count("*").alias("total")
    )
    children_count_df = (
        sdf.filter(col("age") < 16)
        .groupBy("race", "gender")
        .agg(count("*").alias("children"))
    )

    joined_df = total_count_df.join(
        children_count_df, ["race", "gender"], how="left"
    ).fillna(0, subset=["children"])

    return joined_df.withColumn(
        "children_percentage",
        spark_round((col("children") / col("total")) * 100, 2),
    ).select("race", "gender", "children_percentage")


# This data mart derives from fact_population
def generate_data_mart_unemployed_percentage_by_age_group_gender(
    sdf: DataFrame,
) -> DataFrame:
    working_age_df = sdf.filter(
        (col("age") >= 15) & (col("employment").isNotNull())
    )

    total_df = working_age_df.groupBy("age_group", "gender").agg(
        count("*").alias("total")
    )
    unemployed_df = (
        working_age_df.filter(col("employment") != "employed")
        .groupBy("age_group", "gender")
        .agg(count("*").alias("unemployed"))
    )

    joined_df = total_df.join(
        unemployed_df, ["age_group", "gender"], how="left"
    ).fillna(0, subset=["unemployed"])

    return joined_df.withColumn(
        "unemployed_percentage",
        spark_round((col("unemployed") / col("total")) * 100, 2),
    ).select("age_group", "gender", "unemployed_percentage")


# This data mart derives from fact_population
def generate_data_mart_education_distribution_among_unemployed(
    sdf: DataFrame,
) -> DataFrame:
    return (
        sdf.filter(col("employment") != "employed")
        .filter(~col("age_group").isin("child", "adolescent"))
        .groupBy("edu")
        .agg(count("*").alias("unemployed_count"))
        .orderBy(col("unemployed_count").desc())
    )


