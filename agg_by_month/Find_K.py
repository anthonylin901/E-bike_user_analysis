from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt


# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("KMeans Clustering with Visualization") \
    .getOrCreate()

# 加载数据
df = spark.read.csv(r"C:\Users\Hy-AnthonyLin\Desktop\Python\Side Project\Flex\agg_by_month\agg_by_month(group_by_user_pair_id).csv", header=True, inferSchema=True)

# 准备数据
assembler = VectorAssembler(inputCols=["total_distance", "total_trip_time"], outputCol="features")
assembled_df = assembler.transform(df)

# 创建空列表以存储不同K值的WSS和轮廓系数
wss = []
silhouette_scores = []
k_values = range(2, 11)  # 测试从2到10的K值

for k in k_values:
    # K-means聚类
    kmeans = KMeans(k=k, seed=1, featuresCol='features', predictionCol='prediction')
    model = kmeans.fit(assembled_df)
    predictions = model.transform(assembled_df)

    # 计算WSSSE
    wss.append(model.summary.trainingCost)

    # 计算轮廓系数
    evaluator = ClusteringEvaluator(metricName='silhouette', distanceMeasure='squaredEuclidean', predictionCol='prediction')
    silhouette_scores.append(evaluator.evaluate(predictions))


# 绘制肘部法则图
plt.figure()
plt.plot(k_values, wss, marker='o')
plt.xlabel('K')
plt.ylabel('WSS')
plt.title('Elbow Method')
plt.show()

# 绘制轮廓系数图
plt.figure()
plt.plot(k_values, silhouette_scores, marker='o')
plt.xlabel('K')
plt.ylabel('Silhouette Coefficient')
plt.title('Silhouette')
plt.show()
