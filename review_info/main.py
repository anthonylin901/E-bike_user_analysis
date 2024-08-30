import os
import json
import csv

review_info_path = r"C:\Users\Hy-AnthonyLin\Desktop\Pyspark\review_info"

def read_json_file(absolute_path):
    all_data = []
    for json_file in file_path_list(absolute_path):
        extended_data = extend_json_file(json_file)
        if extended_data:
            all_data.extend(extended_data)
    return all_data


def file_path_list(absolute_path):
    file_list = []
    for root, dirs, files in os.walk(absolute_path):
        for file in files:
            if file.endswith(".json"):  # 只处理 JSON 文件
                file_path = os.path.join(root, file)
                file_list.append(file_path)
    return file_list


def extend_json_file(file_path):
    with open(file_path, 'r') as f:
        df = json.load(f)
        data = df.get('data', [])
        _path = df.get('_path', '')  # 从整个 JSON 对象中提取 `_path`
        if not data:
            return []

    processed_data = []
    pair_id = _path.split("/")[1]
    _id = _path.split("/")[3]
    for detail in data:
        # 确保 `detail` 是一个字典并且不是 `None`
        if isinstance(detail, dict):
            processed_data.append({
                "pair_id": pair_id,
                "lng": detail.get("lng"),
                "lat": detail.get("lat")
            })
        else:
            print(_id)
            print("Warning: Skipping non-dictionary item in data:", detail)

    return processed_data


def save_to_csv(data, output_file):
    if not data:
        print("No data to write.")
        return
    fieldnames = ["pair_id", "lng", "lat"]
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    data = read_json_file(review_info_path)
    print(data)
    # save_to_csv(data, "review_info.csv")
