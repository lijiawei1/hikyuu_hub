# 扩展数据工具类
# 生成通达信MA50的数据文件
# 数据结构引用：https://m.55188.com/thread-24998030-1-1.html

# extdata_1.IDX索引文件，每条记录29字节，对应一个股票代码，按股票代码(数值化)升序连续存放
# 2)
# 偏移量Hex    数据含义                                数据类型
# 00 ~01        市场类型2字节(16位);深圳00，上海01       16位整数
# 02~08         股票代码7字节数;6个字符，最后一位00       文本
# 09~18         16字节，00                            整数
# 19~1C         每个股票数据记录个数4字节 (32位)         备注
# 共N个股票

# (3)extdata_1.dat，扩展数据文件，每个数据占用12字节，与数据记录个数相乘就是每只股票的总记录数，所有故据按照extdata1.IDx索引文件中的排列顺序连续存放。


# 偏移量Hex    数据含义                                                                   数据类型     备注
# 00~03         数据日期4字节，例如20240905(2024年9月5日)                                   整数
# 04~07         数据时间4字节;例如144200(14:42:00)，以分钟为计算周期时，此项才会有数值。           整数
# 08~0B         4字节浮点值
#
#               (1)保存排名序号。勾选【生成横向排名数据】选项:计算指标数值，用该数值排序，           float
#               股票在选定【计算品种】中的排名。该序号受【排名方法】(单选框)影响。
#               (2)保存选定的指标数值。不勾选【生成横向排名数据】选项:计算该指标数值
#
# 共N条记录
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
项目工具函数集合
包含各种常用的辅助功能
"""

import os
import sys
import logging
from datetime import datetime
import json
import struct
from typing import List, Dict
import pandas as pd
import numpy as np

from narwhals import Datetime

# 配置日志
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def setup_logging(log_level=logging.INFO):
    """配置项目日志"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('app.log')
        ]
    )




def write_file_dat(file_path, data, mode='r+b', encoding='utf-8'):
    """安全写入数据文件"""
    try:
        with open(file_path, mode, encoding=encoding) as f:
            for item in data:
                # 每个数据项包含：整数1、整数2、4字节浮点数
                # 使用struct.pack打包为二进制格式，格式符含义：
                # I: 4字节无符号整数, I: 4字节无符号整数, f: 浮点数，需要转换为4字节浮点数
                packed_data = struct.pack('IIff', item[0], item[1], item[2])
                f.write(packed_data)
        return True
    except Exception as e:
        logger.error(f"写入文件错误: {e}")
        return False


def write_file_info_batch(file_path, indexes, field_offset, fmt, values):

    # info 记录
    INFO_RECORD_SIZE = 293
    # 转换
    offsets = np.array(indexes) * INFO_RECORD_SIZE + field_offset
    print(offsets)
    write_file_info_inner(file_path, offsets, fmt, values)


def write_file_info_inner(file_path, offsets, fmt, values):
    logger.debug(f"修改文件：{file_path}，偏移量列表: {offsets}，写入值：{values}")
    try:
        with open(file_path, 'r+b') as f:

            for offset in offsets:

                packed_data = struct.pack(fmt, *values)
                f.seek(offset)
                f.write(packed_data)

            logger.debug(f"写入成功")
            return True
    except Exception as e:
        logger.error(f"写入文件错误: {e}")
        return False


def write_file_info(file_path, create: datetime, mode='wb'):

    print("待写入时间：" + create.strftime('%Y-%m-%d %H:%M:%S'))
    try:
        with open(file_path, 'r+b') as f:

            date_int = int(create.strftime("%Y%m%d"))
            time_int = int(create.strftime("%H%M%S"))

            print(date_int)
            print(time_int)
            packed_data = struct.pack('II', date_int, time_int)
            f.seek(0x42)
            f.write(packed_data)

            return True
    except Exception as e:
        logger.error(f"写入文件错误: {e}")
        return False

def parse_file_info(file_path) -> List[Dict]:
    """
    解析固定记录长度的二进制文件

    Args:
        file_path: 二进制文件路径

    Returns:
        包含所有解析记录的列表
    """
    records = []

    try:
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        # 每个记录大小
        RECORD_SIZE = 293

        # 计算记录数量
        num_records = file_size // RECORD_SIZE
        logger.debug(f"文件大小: {file_size} 字节，每个记录大小: {RECORD_SIZE} 字节，记录数量: {num_records}")

        # 示例格式定义（需要根据实际文件结构调整）
        record_format = (
            '<'  # 厉害，大端序和小端序还有区别
            'H'  # 2
            '64s' # 64+2=66
            'I'  # 数据生成日期 4+66=70
            'I'  # 数据生成时间 4+70=74
            'I'  # 毫秒数 4+70=78
            '83x'   # 未知字段 1
                    # 指标公式名称 14
                    # 参数设置 64
                    # 参数个数 1
                    # 计算周期 2
                    # 计算时段 1
                    # 跳过解析 1+14+64+1+2+1=83
            'I'     # 计算时段（日期范围） 开始日期
            'I'     # 结束日期
            '124x'
        )

        # 计算格式字符串对应的字节数
        format_size = struct.calcsize(record_format)
        if format_size != RECORD_SIZE:
            print(f"警告: 格式大小({format_size})与记录大小({RECORD_SIZE})不匹配!")

        # 打开二进制文件读取
        with open(file_path, 'rb') as f:
            for record_index in range(num_records):
                try:
                    # 读取一个记录
                    data = f.read(RECORD_SIZE)

                    if len(data) < RECORD_SIZE:
                        print(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    # 解析二进制数据
                    parsed_data = struct.unpack(record_format, data)

                    # 将解析的数据转换为字典
                    record_dict = {
                        'i': record_index,
                        'seq': parsed_data[0],
                        'name': parsed_data[1].decode('gb2312', errors='ignore').rstrip('\x00'),
                        'date_int': parsed_data[2],
                        'time_int': parsed_data[3],
                        # 'time_int': parsed_data[4],
                        'date_start': parsed_data[5],
                        'date_end': parsed_data[6],
                        'raw_hex': data.hex()  # 保留原始十六进制数据
                    }

                    records.append(record_dict)

                    # 打印每个记录的信息（可选）
                    print(f"\n记录 {record_index}:")
                    for key, value in record_dict.items():
                        if key != 'raw_hex':  # 不打印完整的十六进制数据
                            print(f"  {key}: {value}")

                except struct.error as e:
                    print(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取文件错误: {e}", exc_info=True)
        return []

    return records


def parse_file_idx(file_path) -> List[Dict]:
    records = []

    try:
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        # 每个记录大小
        RECORD_SIZE = 29

        # 计算记录数量
        num_records = file_size // RECORD_SIZE
        logger.debug(f"文件大小: {file_size} 字节，每个记录大小: {RECORD_SIZE} 字节，记录数量: {num_records}")

        # 示例格式定义（需要根据实际文件结构调整）
        record_format = (
            '<'   # 厉害，大端序和小端序还有区别
            'H'   # 市场类型 2字节 无符号整数
            '7s'  # 股票代码
            '16x' # 00
            'I'   # 4
        )

        # 计算格式字符串对应的字节数
        format_size = struct.calcsize(record_format)
        if format_size != RECORD_SIZE:
            logger.error(f"警告: 格式大小({format_size})与记录大小({RECORD_SIZE})不匹配!")

        # 打开二进制文件读取
        with open(file_path, 'rb') as f:
            for record_index in range(num_records):
                try:
                    # 读取一个记录
                    data = f.read(RECORD_SIZE)

                    if len(data) < RECORD_SIZE:
                        print(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    # 解析二进制数据
                    parsed_data = struct.unpack(record_format, data)

                    # 将解析的数据转换为字典
                    record_dict = {
                        'i': record_index,
                        'market_code': parsed_data[0],
                        'stock_code': parsed_data[1].decode('gb2312', errors='ignore').rstrip('\x00'),
                        # 'raw_hex': parsed_data[2],
                        'record_count': parsed_data[2],
                    }

                    records.append(record_dict)

                    # 打印每个记录的信息（可选）
                    print(f"\n记录 {record_index}:")
                    for key, value in record_dict.items():
                        if key != 'raw_hex':  # 不打印完整的十六进制数据
                            print(f"  {key}: {value}")

                except struct.error as e:
                    logger.error(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取索引文件错误: {e}", exc_info=True)
        return []

    return records

def parse_file_dat(file_path) -> List[Dict]:
    records = []

    try:
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        # 每个记录大小
        RECORD_SIZE = 12

        # 计算记录数量
        num_records = file_size // RECORD_SIZE
        logger.debug(f"文件大小: {file_size} 字节，每个记录大小: {RECORD_SIZE} 字节，记录数量: {num_records}")

        # 示例格式定义（需要根据实际文件结构调整）
        record_format = (
            '<'   # 厉害，大端序和小端序还有区别
            'I'   # 市场类型 2字节 无符号整数
            'I'   # 股票代码
            'f'   # 4字节浮点值
        )

        # 计算格式字符串对应的字节数
        format_size = struct.calcsize(record_format)
        if format_size != RECORD_SIZE:
            logger.error(f"警告: 格式大小({format_size})与记录大小({RECORD_SIZE})不匹配!")

        # 打开二进制文件读取
        with open(file_path, 'rb') as f:
            for record_index in range(num_records):
                try:
                    # 读取一个记录
                    data = f.read(RECORD_SIZE)

                    if len(data) < RECORD_SIZE:
                        print(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    # 解析二进制数据
                    parsed_data = struct.unpack(record_format, data)

                    # 将解析的数据转换为字典
                    record_dict = {
                        # 'i': record_index,
                        'date_int': parsed_data[0],
                        'time_int': parsed_data[1],
                        'value_f': parsed_data[2],
                    }

                    records.append(record_dict)

                    # 打印每个记录的信息（可选）
                    # print(f"\n记录 {record_index}:")
                    # for key, value in record_dict.items():
                    #     if key != 'raw_hex':  # 不打印完整的十六进制数据
                    #         print(f"  {key}: {value}")

                except struct.error as e:
                    logger.error(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取索引文件错误: {e}", exc_info=True)
        return []

    return records

def parse_file_dat_by_idx(file_path, records: []) -> List[Dict]:
    records = []

    try:
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        # 每个记录大小
        RECORD_SIZE = 29

        # 计算记录数量
        num_records = file_size // RECORD_SIZE
        logger.debug(f"文件大小: {file_size} 字节，每个记录大小: {RECORD_SIZE} 字节，记录数量: {num_records}")

        # 示例格式定义（需要根据实际文件结构调整）
        record_format = (
            '<'   # 厉害，大端序和小端序还有区别
            'H'   # 市场类型 2字节 无符号整数
            '7s'  # 股票代码
            '16x' # 00
            'I'   # 4
        )

        # 计算格式字符串对应的字节数
        format_size = struct.calcsize(record_format)
        if format_size != RECORD_SIZE:
            logger.error(f"警告: 格式大小({format_size})与记录大小({RECORD_SIZE})不匹配!")

        # 打开二进制文件读取
        with open(file_path, 'rb') as f:
            for record_index in range(num_records):
                try:
                    # 读取一个记录
                    data = f.read(RECORD_SIZE)

                    if len(data) < RECORD_SIZE:
                        print(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    # 解析二进制数据
                    parsed_data = struct.unpack(record_format, data)

                    # 将解析的数据转换为字典
                    record_dict = {
                        'i': record_index,
                        'market_code': parsed_data[0],
                        'stock_code': parsed_data[1].decode('gb2312', errors='ignore').rstrip('\x00'),
                        # 'raw_hex': parsed_data[2],
                        'record_count': parsed_data[2],
                    }

                    records.append(record_dict)

                    # 打印每个记录的信息（可选）
                    #print(f"\n记录 {record_index}:")
                    #for key, value in record_dict.items():
                    #    if key != 'raw_hex':  # 不打印完整的十六进制数据
                    #        print(f"  {key}: {value}")

                except struct.error as e:
                    logger.error(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取索引文件错误: {e}", exc_info=True)
        return []

    return records


def read_file_info(file_path, encoding='utf-8'):
    try:
        # 读取二进制文件
        with open(file_path, 'rb') as f:
            data = f.read()

        # 1.数据序号
        seq = struct.unpack('<H', data[0:2])[0]
        print(f"序号: {seq}")
        text_bytes = struct.unpack('64s', data[2:66])[0]
        text = text_bytes.decode(encoding).rstrip('\x00')
        print(f"文本：{text}")

        date_int = struct.unpack('<I', data[66:70])[0]
        print(f"日期：{date_int}")
        time_int = struct.unpack('<I', data[70:74])[0]
        print(f"时间：{time_int}")
        millis_int = struct.unpack('<I', data[74:78])[0]
        print(f"毫秒：{millis_int}")

        c_char = struct.unpack('c', data[78:79])[0]
        print(f"未知：{c_char}")
        text_bytes = struct.unpack('14s', data[79:93])[0]
        text = text_bytes.decode(encoding).rstrip('\x00')
        print(f"指标公式名称：{text}")

        # 参数设置-设置计算参数  byte 1

        # 计算周期 16位整数 2

        # 计算时段 byte 1

        # 16个参数，4字节的32位浮点数
        # text_bytes = struct.unpack('', data[93:157])[0]
        # text = text_bytes.decode(encoding).rstrip('\x00')
        # print(f"指标公式名称：{text}")

        # TODO 只需要修改
        #  42-45的数据生产日期
        #  46-49的数据生产时间

        # 遍历数据项（假设每项占32字节）
        # for i in range(header):
        #     item_start = 4 + i * 32
        #     item_data = struct.unpack('8sffI', data[item_start:item_start + 20])  # 示例格式
        #     print(f"指标名: {item_data[0].decode('gbk')}, 数值: {item_data[1]}")
    except Exception as e:
        logger.error(f"读取文件错误: {e}")
        return False


def update_and_append_df(df1, df2, n=10, m=20):
    """
    更新dataframe1：用df2的最后N条数据替换或追加到df1

    Args:
        df1: 原始DataFrame
        df2: 新数据DataFrame
        n: 取df2的最后n条数据
        m: 返回更新后df1的最后m条数据

    Returns:
        更新后的DataFrame（最后m条）
    """
    # 取df2的最后n条数据
    df2_last_n = df2.tail(n).copy()

    # 确保df2有time_int列，如果没有则添加（默认0）
    if 'time_int' not in df2_last_n.columns:
        df2_last_n['time_int'] = 0

    # 重新排列列顺序以匹配df1
    df2_last_n = df2_last_n[df1.columns]

    # 找出需要更新的日期（df2_last_n中存在的日期）
    update_dates = set(df2_last_n['date_int'])

    # 从df1中移除这些日期的记录（如果存在）
    df1_updated = df1[~df1['date_int'].isin(update_dates)]

    # 追加df2_last_n的数据
    df1_updated = pd.concat([df1_updated, df2_last_n], ignore_index=True)

    # 按date_int排序
    df1_updated = df1_updated.sort_values('date_int').reset_index(drop=True)

    # 返回最后m条数据
    return df1_updated.tail(m)


def append_after_max_common_date_robust(df1, df2, keep_original=True):
    """
    增强版本：处理各种边界情况

    Args:
        df1: 原始DataFrame
        df2: 新数据DataFrame
        keep_original: 是否保留原始df1中max_common_date之前的数据
    """
    # 参数验证
    if df1.empty or df2.empty:
        raise ValueError("错误: 输入DataFrame不能为空")

    # 确保有date_int列
    if 'date_int' not in df1.columns or 'date_int' not in df2.columns:
        raise ValueError("错误: DataFrame必须包含date_int列")

    # 排序
    df1 = df1.sort_values('date_int').reset_index(drop=True)
    df2 = df2.sort_values('date_int').reset_index(drop=True)

    # 找到相同的日期
    common_dates = set(df1['date_int']).intersection(set(df2['date_int']))

    if not common_dates:
        raise ValueError("错误: 两个DataFrame中没有相同的date_int值")

    max_common_date = max(common_dates)

    print(f"最大重叠日期：{max_common_date}")

    # 获取df2中该日期之后的数据
    df2_to_append = df2[df2['date_int'] >= max_common_date].copy()

    # 添加缺失的列
    for col in df1.columns:
        if col not in df2_to_append.columns:
            if col == 'time_int':
                df2_to_append[col] = 0
            else:
                df2_to_append[col] = None

    # 确保列顺序一致
    df2_to_append = df2_to_append[df1.columns]

    if keep_original:
        # 保留原始df1中max_common_date之前的数据
        df1_to_keep = df1[df1['date_int'] < max_common_date]
        result_df = pd.concat([df1_to_keep, df2_to_append], ignore_index=True)
    else:
        # 完全用df2的数据替换
        result_df = df2_to_append

    # 排序和去重（确保数据一致性）
    result_df = result_df.sort_values('date_int')
    result_df = result_df.drop_duplicates(subset='date_int', keep='last')
    result_df = result_df.reset_index(drop=True)

    return result_df


def append_and_get_tail(df1, df2, m=20):
    """
    追加数据并返回最后M条数据
    """
    # 先进行追加操作
    result_df = append_after_max_common_date_robust(df1, df2)

    # 返回最后m条数据
    m = min(m, len(result_df))
    return result_df.tail(m)


def generate_file_dat(df, file_path):
    """
    将数据按格式还原为通达信扩展数据：extdata_1.dat
    data数据格式：整数、整数、浮点数
    """
    print(df)
    try:
        with open(file_path, 'wb') as f:
            # 遍历dataframe

            # 遍历dataframe
            for _, row in df.iterrows():
                # 从DataFrame行中获取数据
                date_int = int(row['date_int'])
                time_int = int(row.get('time_int', 0))  # 默认为0
                value_f = row['value_f']
                # 每个数据项包含：整数1、整数2、4字节浮点数
                # 使用struct.pack打包为二进制格式，格式符含义：
                # I: 4字节无符号整数, I: 4字节无符号整数, f: 浮点数，需要转换为4字节浮点数
                packed_data = struct.pack('IIf', date_int, time_int, value_f)
                f.write(packed_data)
    except Exception as e:
        logger.error(f"生成dat文件错误：{e}", exc_info=True)
        return False
