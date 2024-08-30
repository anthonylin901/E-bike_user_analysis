import os
import json
import csv

path = r"C:\Users\Hy-AnthonyLin\Desktop\Pyspark\profile"


def create_gender_column(data):
    """确保 'gender' 列存在，值为 None 时设置为 None"""
    if 'gender' not in data:
        data['gender'] = None
    return data

def create_height_cm_column(data):
    """确保 'height_cm' 列存在，值为 None 时设置为 None"""
    if 'height_cm' not in data:
        data['height_cm'] = None
    return data

def create_weight_kg_column(data):
    """确保 'weight_kg' 列存在，值为 None 时设置为 None"""
    if 'weight_kg' not in data:
        data['weight_kg'] = None
    return data

def create_country_column(data):
    """从 'timezone' 列提取国家，并确保 'country' 列存在"""
    if 'timezone' in data:
        data['country'] = data['timezone'].split('/')[0]
    else:
        data['country'] = None
    return data

def create_city_column(data):
    try:
        if 'timezone' in data:
            data['city'] = data['timezone'].split('/')[1]
        else:
            data['city'] = None
    except:
        data['city'] = None
    return data

def process_json_file(file_path):
    """处理单个 JSON 文件"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    data = create_gender_column(data)
    data = create_height_cm_column(data)
    data = create_weight_kg_column(data)
    data = create_country_column(data)
    data = create_city_column(data)

    # 如果 'bikes' 列为空，则返回 None
    if not data.get('bikes'):
        return []

    result = {
        "_id": data.get("_id"),
        "gender": data.get("gender"),
        "height_cm": data.get("height_cm"),
        "weight_kg": data.get("weight_kg"),
        "timezone": data.get("timezone"),
        "country": data.get("country"),
        "city": data.get("city"),
    }
    return result

def read_profile_json_file(absolute_path):
    all_data = []
    for root, dirs, files in os.walk(absolute_path):
        for file in files:
            file_path = os.path.join(root, file)
            processed_data = process_json_file(file_path)
            print(processed_data)
            if processed_data:
                all_data.append(processed_data)
    print(all_data)
    return all_data


def save_to_csv(data, output_file):

    if not data:
        raise ValueError("No data to write to CSV")

    # 获取字段名
    fieldnames = data[0].keys() if isinstance(data[0], dict) else []

    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    # 读取 JSON 文件并转换为数据列表
    data = read_profile_json_file(path)
    # 保存数据为 CSV 文件
    save_to_csv(data, "profile.csv")
