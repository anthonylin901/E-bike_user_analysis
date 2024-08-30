import pandas as pd
from sklearn.cluster import KMeans
from main import read_json_file


# 读取 agg_by_month_cluster CSV 文件
agg_by_month_df = pd.read_csv(r"C:\Users\Hy-AnthonyLin\Desktop\Python\Side Project\Flex\agg_by_month\agg_by_month_cluster_seed_6.csv")

# 过滤出 cluster 为 2 或 3 的 pair_id
filtered_df = agg_by_month_df[(agg_by_month_df['cluster'] == 3)]
pair_ids_to_cluster = filtered_df['pair_id'].unique()

# 读取 review_info.csv 文件
review_df = pd.read_csv(r"C:\Users\Hy-AnthonyLin\Desktop\Python\Side Project\Flex\review_info\review_info.csv")

# 转换 lat 和 lng 列为浮点数
review_df['lat'] = review_df['lat'].astype(float)
review_df['lng'] = review_df['lng'].astype(float)

all_predictions = []
all_centers = []

for pair_id in pair_ids_to_cluster:
    print(pair_id)
    # 过滤出当前 pair_id 的数据
    group_df = review_df[review_df['pair_id'] == pair_id]

    # 如果样本数不足以进行 k-means 聚类，则跳过
    if len(group_df) < 4:
        continue

    # 准备特征向量
    X = group_df[['lat', 'lng']].values

    # 进行 K-means 聚类
    kmeans = KMeans(n_clusters=4, random_state=1)
    kmeans.fit(X)

    # 将聚类结果添加到原始数据中，使用 .loc 避免 SettingWithCopyWarning
    group_df.loc[:, 'prediction'] = kmeans.labels_
    all_predictions.append(group_df)

    # 获取聚类中心点，并添加 prediction 列
    centers_df = pd.DataFrame(kmeans.cluster_centers_, columns=["lat_center", "lng_center"])
    centers_df["pair_id"] = pair_id
    centers_df["prediction"] = range(kmeans.n_clusters)  # 添加 prediction 列
    all_centers.append(centers_df)

# 合并所有的预测结果和中心点
if all_predictions:
    combined_predictions = pd.concat(all_predictions, ignore_index=True)
    combined_predictions.to_csv("cluster3_predictions.csv", index=False)

if all_centers:
    combined_centers = pd.concat(all_centers, ignore_index=True)
    combined_centers.to_csv("cluster3_centers.csv", index=False)
