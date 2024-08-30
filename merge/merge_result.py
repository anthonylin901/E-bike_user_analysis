import csv


def read_csv(file_path):
    """读取 CSV 文件并将数据存储到字典中"""
    data = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data


def join_data(user_pair_id_data, profile_data):
    """根据 `_id` 列进行左连接"""
    # 创建以 `_id` 为键的字典
    profile_dict = {row['_id']: row for row in profile_data}

    # 生成合并后的结果
    merged_data = []
    for user_row in user_pair_id_data:
        profile_row = profile_dict.get(user_row['_id'])
        if profile_row:
            # 如果在 profile_data 中找到匹配的行
            merged_row = {**user_row, **profile_row}
        else:
            # 如果在 profile_data 中找不到匹配的行
            merged_row = user_row.copy()  # 复制 user_row 数据
            for key in profile_dict[next(iter(profile_dict))].keys():
                if key != '_id':  # 确保不覆盖 _id
                    merged_row[key] = None  # 未找到的字段设为 None
        merged_data.append(merged_row)

    return merged_data



def save_to_csv(data, output_file):
    """将数据保存为 CSV 文件"""
    if not data:
        return

    fieldnames = data[0].keys()
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


if __name__ == "__main__":
    user_pair_id_file = 'user_pair_id/user_pair_id.csv'
    profile_file = 'profile/profile.csv'
    output_file = 'merged_result.csv'

    # 读取 CSV 文件
    user_pair_id_data = read_csv(user_pair_id_file)
    profile_data = read_csv(profile_file)

    # 执行 join 操作
    merged_data = join_data(user_pair_id_data, profile_data)

    # 保存结果到 CSV 文件
    save_to_csv(merged_data, output_file)
    print(f"Data successfully merged and saved to {output_file}")
