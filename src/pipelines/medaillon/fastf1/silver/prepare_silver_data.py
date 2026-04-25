from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class PrepareSilverData:
    def prepare(self, df: DataFrame) -> DataFrame:
        df = self._rename_columns(df)
        df = self._convert_timedelta_columns_to_ms(df)
        df = self._cast_columns(df)
        df = self._handle_nulls(df)
        df = self._add_basic_flags(df)
        return df

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        rename_mapping = {
            "laptime": "lap_time_ms",
            "sector1time": "sector1_time_ms",
            "sector2time": "sector2_time_ms",
            "sector3time": "sector3_time_ms",
            "pitintime": "pit_in_time_ms",
            "pitouttime": "pit_out_time_ms",
            "lapstarttime": "lap_start_time_ms",
            "trackstatus": "track_status",
            "tyrelife": "tyre_life",
            "freshtyre": "fresh_tyre",
            "isaccurate": "is_accurate",
            "drivernumber": "driver_number",
            "airtemp": "air_temp",
            "tracktemp": "track_temp",
            "winddirection": "wind_direction",
            "windspeed": "wind_speed",
        }

        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        return df

    def _timedelta_string_to_ms_expr(self, col_name: str):
        col_str = F.trim(F.col(col_name).cast("string"))

        days = F.regexp_extract(col_str, r"(\d+)\s+days?", 1)
        hhmmss = F.regexp_extract(col_str, r"(\d{2}:\d{2}:\d{2}(?:\.\d+)?)", 1)

        hours = F.regexp_extract(hhmmss, r"^(\d{2})", 1)
        minutes = F.regexp_extract(hhmmss, r"^\d{2}:(\d{2})", 1)
        seconds = F.regexp_extract(hhmmss, r"^\d{2}:\d{2}:(\d{2}(?:\.\d+)?)$", 1)

        total_ms = (
            F.coalesce(days.cast("double"), F.lit(0.0)) * F.lit(86400000.0) +
            F.coalesce(hours.cast("double"), F.lit(0.0)) * F.lit(3600000.0) +
            F.coalesce(minutes.cast("double"), F.lit(0.0)) * F.lit(60000.0) +
            F.coalesce(seconds.cast("double"), F.lit(0.0)) * F.lit(1000.0)
        )

        return (
            F.when(F.col(col_name).isNull(), None)
             .when(col_str == "", None)
             .when(col_str.rlike(r"^\d+(\.\d+)?$"), col_str.cast("double"))
             .when(col_str.rlike(r"^\d+\s+days?\s+\d{2}:\d{2}:\d{2}(\.\d+)?$"), total_ms)
             .when(col_str.rlike(r"^\d{2}:\d{2}:\d{2}(\.\d+)?$"), total_ms)
             .otherwise(None)
        )

    def _convert_timedelta_columns_to_ms(self, df: DataFrame) -> DataFrame:
        timedelta_cols = [
            "lap_time_ms",
            "sector1_time_ms",
            "sector2_time_ms",
            "sector3_time_ms",
            "pit_in_time_ms",
            "pit_out_time_ms",
            "lap_start_time_ms",
            "q1",
            "q2",
            "q3",
            "time",
        ]

        for col_name in timedelta_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, self._timedelta_string_to_ms_expr(col_name))

        return df

    def _safe_cast_int(self, df: DataFrame, col_name: str) -> DataFrame:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast("int"))
        return df

    def _safe_cast_double(self, df: DataFrame, col_name: str) -> DataFrame:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.expr(f"try_cast({col_name} as double)"))
        return df

    def _cast_columns(self, df: DataFrame) -> DataFrame:
        int_cols = [
            "lap_number",
            "position",
            "tyre_life",
            "stint",
            "grid_position",
            "finish_position",
            "laps",
        ]

        double_cols = [
            "lap_time_ms",
            "sector1_time_ms",
            "sector2_time_ms",
            "sector3_time_ms",
            "pit_in_time_ms",
            "pit_out_time_ms",
            "lap_start_time_ms",
            "speedi1",
            "speedi2",
            "speedfl",
            "speedst",
            "q1",
            "q2",
            "q3",
            "time",
            "points",
            "air_temp",
            "humidity",
            "pressure",
            "track_temp",
            "wind_direction",
            "wind_speed",
        ]

        for col_name in int_cols:
            df = self._safe_cast_int(df, col_name)

        for col_name in double_cols:
            df = self._safe_cast_double(df, col_name)

        bool_cols = [
            "fresh_tyre",
            "rainfall",
            "deleted",
            "fastf1generated",
            "is_accurate",
        ]

        for col_name in bool_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("boolean"))

        return df

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        if "lap_time_ms" in df.columns:
            df = df.filter(F.col("lap_time_ms").isNotNull())

        fill_false_cols = [
            "fresh_tyre",
            "rainfall",
            "deleted",
            "fastf1generated",
            "is_accurate",
        ]

        for col_name in fill_false_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(False)))

        if "compound" in df.columns:
            df = df.withColumn("compound", F.coalesce(F.col("compound"), F.lit("UNKNOWN")))

        if "track_status" in df.columns:
            df = df.withColumn("track_status", F.coalesce(F.col("track_status"), F.lit("")))

        return df

    def _add_basic_flags(self, df: DataFrame) -> DataFrame:
        if "pit_in_time_ms" in df.columns:
            df = df.withColumn("is_pit_in_lap", F.col("pit_in_time_ms").isNotNull())
        else:
            df = df.withColumn("is_pit_in_lap", F.lit(False))

        if "pit_out_time_ms" in df.columns:
            df = df.withColumn("is_pit_out_lap", F.col("pit_out_time_ms").isNotNull())
        else:
            df = df.withColumn("is_pit_out_lap", F.lit(False))

        df = df.withColumn("is_pit_lap", F.col("is_pit_in_lap") | F.col("is_pit_out_lap"))

        if "track_status" in df.columns:
            track_status = F.coalesce(F.col("track_status").cast("string"), F.lit(""))

            df = df.withColumn("is_green", track_status.rlike(r"(^|;)1($|;)"))
            df = df.withColumn("is_yellow", track_status.rlike(r"(^|;)2($|;)"))
            df = df.withColumn("is_safety_car", track_status.rlike(r"(^|;)4($|;)"))
            df = df.withColumn("is_red_flag", track_status.rlike(r"(^|;)5($|;)"))
            df = df.withColumn("is_vsc", track_status.rlike(r"(^|;)6($|;)"))
        else:
            df = (
                df.withColumn("is_green", F.lit(False))
                  .withColumn("is_yellow", F.lit(False))
                  .withColumn("is_safety_car", F.lit(False))
                  .withColumn("is_red_flag", F.lit(False))
                  .withColumn("is_vsc", F.lit(False))
            )

        if "rainfall" in df.columns:
            df = df.withColumn("is_raining", F.coalesce(F.col("rainfall"), F.lit(False)))
        else:
            df = df.withColumn("is_raining", F.lit(False))

        return df