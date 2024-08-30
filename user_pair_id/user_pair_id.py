import os
import json
import csv

path = r"C:\Users\Hy-AnthonyLin\Desktop\Pyspark\user_bikes_pairing"

def file_path_list(absolute_path):
    """获取所有 JSON 文件的路径"""
    file_list = []
    for root, dirs, files in os.walk(absolute_path):
        for file in files:
            if file.endswith(".json"):  # 只处理 JSON 文件
                file_path = os.path.join(root, file)
                file_list.append(file_path)
    return file_list

def process_json_file(file_path):
    """处理单个 JSON 文件"""
    with open(file_path, 'r') as f:
        data = json.load(f)

    # 如果 'bikes' 列为空，则返回 None
    if not data.get('bikes'):
        return []

    # 处理 JSON 数据
    bikes = data.get('bikes', [])
    processed_data = []
    for bike in bikes:
        processed_data.append({
            "_id": data.get("_id"),
            "bike_id": bike.get("bike_id"),
            "pair_id": bike.get("pair_id")
        })

    return processed_data


def read_user_pair_id_json_file(absolute_path):
    """读取 JSON 文件并处理为 CSV"""
    all_data = []
    for json_file in file_path_list(absolute_path):
        processed_data = process_json_file(json_file)
        if processed_data:
            all_data.extend(processed_data)

    return all_data


def save_to_csv(data, output_file):
    """将数据保存为 CSV 文件"""
    if not data:
        print("No data to write.")
        return

    # 获取列名
    fieldnames = ["_id", "bike_id", "pair_id"]

    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    # 读取 JSON 文件并转换为数据列表
    data = read_user_pair_id_json_file(path)

    # 保存数据为 CSV 文件
    save_to_csv(data, "user_pair_id.csv")
