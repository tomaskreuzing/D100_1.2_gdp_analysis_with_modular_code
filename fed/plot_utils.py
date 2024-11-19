import seaborn as sns
import polars as pl

def plot_gdp(df: pl.DataFrame):
    return sns.lineplot(data=df, x="Year", y="GDP", hue="Country Name")