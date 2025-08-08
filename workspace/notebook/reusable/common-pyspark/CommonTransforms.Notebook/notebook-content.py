# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # What is CommonTransforms and how to use them in your notebooks ?  
# CommonTransforms is a Python class that uses PySpark libraries to apply common transformations to a Spark dataframe. https://github.com/bennyaustin/pyspark-utils/blob/main/CommonTransforms/README.md

# MARKDOWN ********************

# # CommonTransforms Class

# CELL ********************

from pyspark.sql.functions import trim,when,isnull,lit,col,from_utc_timestamp,to_utc_timestamp,concat_ws,sha1,length,substring,lit,concat,date_add,expr,year,datediff
from pyspark.sql import functions as F 
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



class CommonTransforms:
    """Common PySpark DataFrame transformations for data cleaning and preparation."""

    def __init__(self, input_df):
        """Initialize with a Spark DataFrame."""
        self.input_df = input_df
        self.input_schema = self.input_df.schema
        self.input_columns = self.input_df.schema.fieldNames()

    def trim(self):
        """Remove leading and trailing spaces from all string columns."""
        string_cols = (col for col in self.input_schema if str(col.dataType) == "StringType")
        for c in string_cols:
            self.input_df = self.input_df.withColumn(c.name, trim(c.name))
        return self.input_df

    def replace_null(self, value, subset=None):
        """Replace null values with a default value based on datatype."""
        is_date = False
        is_timestamp = False

        try:
            if isinstance(value, str):
                datetime.datetime.strptime(value, "%Y-%m-%d")
                is_date = True
        except ValueError:
            is_date = False

        try:
            if isinstance(value, str):
                datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                is_timestamp = True
        except ValueError:
            is_timestamp = False

        if is_date and subset is not None:
            date_cols = (x for x in self.input_schema if str(x.dataType) == "DateType" and x.nullable and x.name in subset)
            for x in date_cols:
                self.input_df = self.input_df.withColumn(x.name, when(isnull(col(x.name)), lit(value)).otherwise(col(x.name)))
        elif is_date and subset is None:
            date_cols = (x for x in self.input_schema if str(x.dataType) == "DateType" and x.nullable)
            for x in date_cols:
                self.input_df = self.input_df.withColumn(x.name, when(isnull(col(x.name)), lit(value)).otherwise(col(x.name)))
        elif is_timestamp and subset is not None:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType" and x.nullable and x.name in subset)
            for x in ts_cols:
                self.input_df = self.input_df.withColumn(x.name, when(isnull(col(x.name)), lit(value)).otherwise(col(x.name)))
        elif is_timestamp and subset is None:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType" and x.nullable)
            for x in ts_cols:
                self.input_df = self.input_df.withColumn(x.name, when(isnull(col(x.name)), lit(value)).otherwise(col(x.name)))
        else:
            self.input_df = self.input_df.fillna(value, subset)

        return self.input_df

    def deduplicate(self, subset=None):
        """Remove duplicate rows from the DataFrame."""
        self.input_df = self.input_df.dropDuplicates(subset)
        return self.input_df

    def utc_to_local(self, local_timezone, subset=None):
        """Convert UTC timestamps to a local timezone."""
        if subset is not None:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType" and x.name in subset)
        else:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType")
        for x in ts_cols:
            self.input_df = self.input_df.withColumn(x.name, from_utc_timestamp(col(x.name), local_timezone))
        return self.input_df

    def local_to_utc(self, local_timezone, subset=None):
        """Convert local timezone timestamps to UTC."""
        if subset is not None:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType" and x.name in subset)
        else:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType")
        for x in ts_cols:
            self.input_df = self.input_df.withColumn(x.name, to_utc_timestamp(col(x.name), local_timezone))
        return self.input_df

    def change_timezone(self, from_timezone, to_timezone, subset=None):
        """Change timezone from one to another for timestamp columns."""
        if subset is not None:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType" and x.name in subset)
        else:
            ts_cols = (x for x in self.input_schema if str(x.dataType) == "TimestampType")
        for x in ts_cols:
            self.input_df = self.input_df.withColumn(x.name, to_utc_timestamp(col(x.name), from_timezone))
            self.input_df = self.input_df.withColumn(x.name, from_utc_timestamp(col(x.name), to_timezone))
        return self.input_df

    def drop_sys_columns(self, columns):
        """Drop system or non-business columns from the DataFrame."""
        self.input_df = self.input_df.drop(columns)
        return self.input_df

    def add_checksum_col(self, col_name):
        """Add a checksum column to the DataFrame."""
        self.input_df = self.input_df.withColumn(col_name, sha1(concat_ws("~~", *self.input_df.columns)))
        return self.input_df

    def julian_to_calendar(self, subset):
        """Convert Julian date columns to calendar date columns."""
        jul_cols = (x for x in self.input_schema if str(x.dataType) == "IntegerType" and x.name in subset)
        for x in jul_cols:
            self.input_df = (
                self.input_df.withColumn(x.name, col(x.name).cast("string"))
                .withColumn(x.name + "_year",
                    when((length(col(x.name)) == 5) & (substring(col(x.name), 1, 2) <= 50), concat(lit('20'), substring(col(x.name), 1, 2)))
                    .when((length(col(x.name)) == 5) & (substring(col(x.name), 1, 2) > 50), concat(lit('19'), substring(col(x.name), 1, 2)))
                    .when(length(col(x.name)) == 7, substring(col(x.name), 1, 4))
                    .otherwise(lit(0))
                )
                .withColumn(x.name + "_days",
                    when(length(col(x.name)) == 5, substring(col(x.name), 3, 3).cast("int"))
                    .when(length(col(x.name)) == 7, substring(col(x.name), 5, 3).cast("int"))
                    .otherwise(lit(0))
                )
                .withColumn(x.name + "_ref_year", concat(col(x.name + "_year"), lit("-01"), lit("-01")).cast("date"))
                .withColumn(x.name + "_calendar", expr("date_add(" + x.name + "_ref_year," + x.name + "_days)-1"))
                .drop(x.name, x.name + "_year", x.name + "_days", x.name + "_ref_year")
                .withColumnRenamed(x.name + "_calendar", x.name)
            )
        return self.input_df

    def calendar_to_julian(self, subset):
        """Convert calendar date columns to Julian date columns."""
        cal_cols = (x for x in self.input_schema if ((str(x.dataType) == "DateType" or str(x.dataType) == "TimestampType") and x.name in subset))
        for x in cal_cols:
            self.input_df = (
                self.input_df.withColumn(x.name + "_ref_year", concat(year(col(x.name)).cast("string"), lit("-01"), lit("-01")))
                .withColumn(x.name + "_datediff", datediff(col(x.name), col(x.name + "_ref_year")) + 1)
                .withColumn(x.name + "_julian", concat(substring(year(col(x.name)).cast("string"), 3, 2), col(x.name + "_datediff")).cast("int"))
                .drop(x.name, x.name + "_ref_year", x.name + "_datediff")
                .withColumnRenamed(x.name + "_julian", x.name)
            )
        return self.input_df

    def add_lit_cols(self, col_dict):
        """Add literal value columns to the DataFrame from a dictionary."""
        for k, v in col_dict.items():
            self.input_df = self.input_df.withColumn(k, lit(v))
        return self.input_df

    @staticmethod
    def flatten_nested(df):
        """Flatten nested JSON or XML structures in a DataFrame."""
        struct_cols = []
        sep = "_"

        for col_name, col_type in df.dtypes:
            if col_type.startswith("struct"):
                struct_cols.append(col_name)

        array_cols = []
        for col_name, col_type in df.dtypes:
            if col_type.startswith("array"):
                array_cols.append(col_name)

        if struct_cols:
            struct_element = []
            for col_name, col_type in df.dtypes:
                if col_type.startswith("struct"):
                    struct_element.append(col_name)
            flatten_cols = [fc for fc, _ in df.dtypes if fc not in struct_element]
            for nc in struct_element:
                for cc in df.select(f"{nc}.*").columns:
                    flatten_cols.append(F.col(f"{nc}.{cc}").alias(f"{nc}{sep}{cc}"))
            df = df.select(flatten_cols)
            return CommonTransforms.flatten_nested(df)

        if array_cols:
            array_element = []
            for col_name, col_type in df.dtypes:
                if col_type.startswith("array"):
                    array_element.append(col_name)
            exploded_df = df
            for nc in array_element:
                exploded_df = exploded_df.withColumn(nc, F.explode_outer(F.col(nc)))
            df = exploded_df
            return CommonTransforms.flatten_nested(df)

        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
