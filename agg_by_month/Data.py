import pandas as pd
import os
import json

agg_by_month_path = r"C:\Users\Hy-AnthonyLin\Desktop\Pyspark\agg_by_month_json\review_info"

def file_path_list(absolute_path):
    """获取所有 JSON 文件的路径"""
    file_list = []
    for root, dirs, files in os.walk(absolute_path):
        for file in files:
            if file.endswith(".json"):  # 只处理 JSON 文件
                file_path = os.path.join(root, file)
                file_list.append(file_path)
    return file_list

def read_user_pair_id_json_file(absolute_path):
    """读取 JSON 文件并处理为 DataFrame"""
    all_data = []
    for json_file in file_path_list(absolute_path):
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 如果 JSON 文件的顶层是一个字典而非列表，转为列表
            if isinstance(data, dict):
                data = [data]
            all_data.extend(data)
    return pd.DataFrame(all_data)

def remove_consecutive_duplicates(df, column_name):
    if column_name in df.columns:
        df['aggregate_start_date'] = pd.to_datetime(df['aggregate_start_date']).dt.to_period('M')
        df['pair_id'] = df['_path'].str.split('/').str.get(1)
        df['previous_value'] = df[column_name].shift(1)
        df_filtered = df[df[column_name] != df['previous_value']]
        df_filtered = df_filtered.drop(columns=['previous_value'])
        return df_filtered

    else:
        print(f"Column '{column_name}' not found in DataFrame.")
        return df

def group_by_table(df):
    grouped_df = df.groupby('pair_id').agg({
        'total_distance': 'sum',
        'total_trip_time': 'sum',
        '_id': 'first'
    }).reset_index()
    return grouped_df

def avg_distance_under_40_km(df):
    df['avg_distance(hr)'] = (df['total_distance'] /1000) / (df['total_trip_time']/3600)
    df = df[df['avg_distance(hr)'] < 40 ]
    return df


def save_to_csv(df, output_file):
    if df.empty:
        print("No data to write.")
        return
    df.to_csv(output_file, index=False)

def main():
    df = read_user_pair_id_json_file(agg_by_month_path)
    df_filtered = remove_consecutive_duplicates(df, 'aggregate_start_date')
    df_filtered = group_by_table(df_filtered)
    df_filtered = avg_distance_under_40_km(df_filtered)
    save_to_csv(df_filtered, "agg_by_month(group_by_user_pair_id).csv")

if __name__ == "__main__":
    main()
