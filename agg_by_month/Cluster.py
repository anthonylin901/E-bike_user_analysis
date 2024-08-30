from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import matplotlib.pyplot as plt


def create_spark_session(app_name="KMeans Clustering with Visualization"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_data(spark, file_path):
    """
    從指定的 CSV 文件加載數據，並返回 DataFrame。
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)


def prepare_data(df, input_cols, output_col="features"):
    """
    使用 VectorAssembler 函式將指定的列轉換為一個 features 列，並返回轉換後的 DataFrame。
    """
    assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col)
    return assembler.transform(df)


def apply_kmeans(df, k=4, seed=1, features_col="features", prediction_col="cluster"):
    """
    應用 K-means 聚類，返回模型和聚類後的 DataFrame。
    """
    kmeans = KMeans(k=k, seed=seed, featuresCol=features_col, predictionCol=prediction_col)
    model = kmeans.fit(df)
    clustered_df = model.transform(df)
    return model, clustered_df


def save_to_csv(df, output_path):
    """
    將 DataFrame 轉換為 Pandas DataFrame 並儲存為 CSV 文件。
    """
    pandas_df = df.toPandas()
    pandas_df.to_csv(output_path, index=False)
    return pandas_df


def plot_clusters(pandas_df, seed):
    """
    使用 Matplotlib 繪製聚類結果。
    """
    plt.figure(figsize=(10, 7))
    plt.scatter(pandas_df['total_distance'], pandas_df['total_trip_time'], c=pandas_df['cluster'], cmap='viridis')
    plt.xlabel('Total Distance')
    plt.ylabel('Total Trip Time')
    plt.title(f'K-means Clustering (k=4, seed={seed})')
    plt.colorbar(label='Cluster')
    plt.show()


def main():
    spark = create_spark_session()
    file_path = r"C:\Users\Hy-AnthonyLin\Desktop\Python\Side Project\Flex\agg_by_month\agg_by_month(group_by_user_pair_id).csv"
    df = load_data(spark, file_path)
    assembled_df = prepare_data(df, input_cols=["total_distance", "total_trip_time"])
    # 遍歷 seed 值從 1 到 4
    for seed in [1,2,3,4,5,6]:
        print(f"Running K-means with seed={seed}")
        # 應用 K-means 聚類
        model, clustered_df = apply_kmeans(assembled_df, seed=seed)
        # 查看聚類結果
        clustered_df.show()
        # 將聚類結果儲存為 CSV
        output_path = f"agg_by_month_cluster_seed_{seed}.csv"
        pandas_df = save_to_csv(
            clustered_df.select("pair_id", "_id", "total_distance", "total_trip_time", "features", "cluster"),
            output_path)
        # 繪製聚類結果
        plot_clusters(pandas_df, seed)
    # 停止 SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
