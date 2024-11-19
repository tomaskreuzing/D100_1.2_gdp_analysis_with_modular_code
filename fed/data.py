import polars as pl #for greater speed than pandas
from pathlib import Path #for reproducibility across machines
from typing import List # for specifying list contents

def read_data(file_name:str, skip_rows: int = 4) -> pl.DataFrame: #using type hints for good practice
    path = Path(__file__).parent.parent / "data" / file_name
    return pl.read_csv(path, skip_rows=skip_rows, null_values="")

def clean_data(df: pl.DataFrame, countries: List[str], start:int, end:int) -> pl.DataFrame:
    filtered_df = df.filter(pl.col("Country Code").is_in(countries)) #filtering for particular countries
    selected_cols = ["Country Name"] + [str(i) for i in range(start, end)] #filtering for particular years, nicely excludes all non-year columns
    filtered_df = filtered_df.select(selected_cols) #keep in mind range does not include the end value
    filtered_df = filtered_df.unpivot(
        index=["Country Name"], variable_name="Year", value_name="GDP" #changing the orientation of the dataset
    )
    filtered_df = filtered_df.with_columns(
        pl.col("Year").cast(pl.Int32),  #this method may cause some issues if you have not properly set missing values to the null format in read_csv
        pl.col("GDP").cast(pl.Float64), 
    )
    return filtered_df.sort("Country Name", "Year") # all years for one country printed first