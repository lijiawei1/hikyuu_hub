# 扩展数据工具类
# 生成通达信MA50的数据文件
# 数据结构引用：https://m.55188.com/thread-24998030-1-1.html

import os
import sys
import logging
from datetime import datetime
import struct
from typing import List, Dict, Optional, Union, Tuple, Any, Set
import pandas as pd
import numpy as np
from tqdm import tqdm

# 配置日志
logger = logging.getLogger(__name__)

# 常量定义
IDX_RECORD_SIZE = 29
DAT_RECORD_SIZE = 12
INFO_RECORD_SIZE = 293


# def setup_logging(log_level=logging.INFO) -> None:
#     """配置项目日志"""
#     logging.basicConfig(
#         level=log_level,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#         handlers=[
#             logging.StreamHandler(),
#             logging.FileHandler('app.log')
#         ]
#     )
#     logger.setLevel(log_level)


def write_binary_data(file_path: str, data: List[Tuple[int, int, float]], mode: str = 'wb') -> bool:
    """安全写入二进制数据文件"""
    try:
        with open(file_path, mode) as f:
            for date_int, time_int, value_f in data:
                packed_data = struct.pack('IIf', date_int, time_int, value_f)
                f.write(packed_data)
        logger.info(f"成功写入数据到文件: {file_path}")
        return True
    except Exception as e:
        logger.error(f"写入文件错误: {file_path}, 错误: {e}")
        return False


def write_file_info_batch(file_path: str, indexes: List[int], field_offset: int,
                          fmt: str, values: Tuple[Any]) -> bool:
    """批量写入文件信息"""
    offsets = np.array(indexes) * INFO_RECORD_SIZE + field_offset
    logger.debug(f"修改文件：{file_path}，偏移量列表: {offsets}，写入值：{values}")
    return _write_file_info_inner(file_path, offsets, fmt, values)


def _write_file_info_inner(file_path: str, offsets: List[int], fmt: str, values: Tuple[Any]) -> bool:
    """内部函数：实际执行文件信息写入"""
    try:
        with open(file_path, 'r+b') as f:
            for offset in offsets:
                packed_data = struct.pack(fmt, *values)
                f.seek(offset)
                f.write(packed_data)
        logger.info(f"成功写入文件信息: {file_path}")
        return True
    except Exception as e:
        logger.error(f"写入文件信息错误: {file_path}, 错误: {e}")
        return False


def write_file_info(file_path: str, create: datetime, mode: str = 'r+b') -> bool:
    """写入文件信息"""
    logger.info(f"待写入时间：{create.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        with open(file_path, mode) as f:
            date_int = int(create.strftime("%Y%m%d"))
            time_int = int(create.strftime("%H%M%S"))
            packed_data = struct.pack('II', date_int, time_int)
            f.seek(0x42)
            f.write(packed_data)
        logger.info(f"成功写入文件信息: {file_path}")
        return True
    except Exception as e:
        logger.error(f"写入文件信息错误: {file_path}, 错误: {e}")
        return False


def parse_binary_file(file_path: str, record_size: int, record_format: str,
                      record_processor: callable) -> List[Dict]:
    """
    通用二进制文件解析函数

    Args:
        file_path: 文件路径
        record_size: 记录大小
        record_format: 结构格式
        record_processor: 记录处理函数

    Returns:
        解析后的记录列表
    """
    records = []

    try:
        file_size = os.path.getsize(file_path)
        num_records = file_size // record_size

        format_size = struct.calcsize(record_format)
        if format_size != record_size:
            logger.warning(f"格式大小({format_size})与记录大小({record_size})不匹配!")

        logger.debug(f"文件大小: {file_size} 字节，记录大小: {record_size} 字节，记录数量: {num_records}")

        with open(file_path, 'rb') as f:
            for record_index in range(num_records):
                try:
                    data = f.read(record_size)
                    if len(data) < record_size:
                        logger.warning(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    parsed_data = struct.unpack(record_format, data)
                    record_dict = record_processor(record_index, parsed_data, data)
                    records.append(record_dict)

                except struct.error as e:
                    logger.error(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取文件错误: {file_path}, 错误: {e}", exc_info=True)
        return []

    return records


def _process_info_record(record_index: int, parsed_data: tuple, raw_data: bytes) -> Dict:
    """处理信息记录"""
    return {
        # 'i': record_index,
        'seq': parsed_data[0],
        'name': parsed_data[1].decode('gb2312', errors='ignore').rstrip('\x00'),
        'date_int': parsed_data[2],
        'time_int': parsed_data[3],
        'date_start': parsed_data[5],
        'date_end': parsed_data[6]
        # 'raw_hex': raw_data.hex()
    }


def _process_idx_record(record_index: int, parsed_data: tuple, raw_data: bytes) -> Dict:
    """处理索引记录"""
    return {
        'i': record_index,
        'market_code': parsed_data[0],
        'stock_code': parsed_data[1].decode('gb2312', errors='ignore').rstrip('\x00'),
        'record_count': parsed_data[2],
    }


def _process_dat_record(record_index: int, parsed_data: tuple, raw_data: bytes) -> Dict:
    """处理数据记录"""
    return {
        'date_int': parsed_data[0],
        'time_int': parsed_data[1],
        'value_f': parsed_data[2],
    }


def parse_file_info(file_path: str) -> List[Dict]:
    """解析信息文件"""
    record_format = (
        '<'  # 小端序
        'H'  # 2字节无符号整数
        '64s'  # 64字节字符串
        'I'  # 4字节无符号整数
        'I'  # 4字节无符号整数
        'I'  # 4字节无符号整数
        '83x'  # 跳过83字节
        'I'  # 4字节无符号整数
        'I'  # 4字节无符号整数
        '124x'  # 跳过124字节
    )
    return parse_binary_file(file_path, INFO_RECORD_SIZE, record_format, _process_info_record)


def parse_file_idx(file_path: str) -> List[Dict]:
    """解析索引文件"""
    record_format = (
        '<'  # 小端序
        'H'  # 2字节无符号整数
        '7s'  # 7字节字符串
        '16x'  # 跳过16字节
        'I'  # 4字节无符号整数
    )
    return parse_binary_file(file_path, IDX_RECORD_SIZE, record_format, _process_idx_record)


def parse_file_dat(file_path: str) -> List[Dict]:
    """解析数据文件"""
    record_format = (
        '<'  # 小端序
        'I'  # 4字节无符号整数
        'I'  # 4字节无符号整数
        'f'  # 4字节浮点数
    )
    return parse_binary_file(file_path, DAT_RECORD_SIZE, record_format, _process_dat_record)


def parse_file_dat2(file_path: str, start_index: int, record_count: int) ->  List[Dict]:
    """
    解析数据文件
    Args:
        file_path: 文件路径
        start_index: 起始索引
        record_count: 记录数量
    Returns:
        解析后的记录列表
    """
    record_format = (
        '<'  # 小端序
        'I'  # 4字节无符号整数
        'I'  # 4字节无符号整数
        'f'  # 4字节浮点数
    )
    return parse_binary_file2(file_path, DAT_RECORD_SIZE, record_format, start_index, record_count, _process_dat_record)


def parse_binary_file2(file_path: str, record_size: int, record_format: str, index: int, record_count: int,
                      record_processor: callable) -> List[Dict]:
    """
    通用二进制文件解析函数

    Args:
        file_path: 文件路径
        record_size: 记录大小
        record_format: 结构格式
        index: 起始索引
        record_count: 记录数量
        record_processor: 记录处理函数
    Returns:
        解析后的记录列表
    """
    records = []

    try:
        file_size = os.path.getsize(file_path)
        num_records = file_size // record_size

        format_size = struct.calcsize(record_format)
        if format_size != record_size:
            logger.warning(f"格式大小({format_size})与记录大小({record_size})不匹配!")

        logger.debug(f"文件大小: {file_size} 字节，记录大小: {record_size} 字节，记录数量: {num_records}")

        with open(file_path, 'rb') as f:

            # 先定位到起始位置：
            f.seek(record_size * index)

            for record_index in range(record_count):
                try:
                    data = f.read(record_size)
                    if len(data) < record_size:
                        logger.warning(f"记录 {record_index}: 数据不足 {len(data)} 字节")
                        break

                    parsed_data = struct.unpack(record_format, data)
                    record_dict = record_processor(record_index, parsed_data, data)
                    records.append(record_dict)

                except struct.error as e:
                    logger.error(f"解析记录 {record_index} 时出错: {e}")
                    break

    except Exception as e:
        logger.error(f"读取文件错误: {file_path}, 错误: {e}", exc_info=True)
        return []

    return records


def update_and_append_df(df1: pd.DataFrame, df2: pd.DataFrame, n: int = 10, m: int = 20) -> pd.DataFrame:
    """
    更新DataFrame：用df2的最后N条数据替换或追加到df1

    Args:
        df1: 原始DataFrame
        df2: 新数据DataFrame
        n: 取df2的最后n条数据
        m: 返回更新后df1的最后m条数据

    Returns:
        更新后的DataFrame（最后m条）
    """
    df2_last_n = df2.tail(n).copy()

    if 'time_int' not in df2_last_n.columns:
        df2_last_n['time_int'] = 0

    df2_last_n = df2_last_n[df1.columns]
    update_dates = set(df2_last_n['date_int'])
    df1_updated = df1[~df1['date_int'].isin(update_dates)]
    df1_updated = pd.concat([df1_updated, df2_last_n], ignore_index=True)
    df1_updated = df1_updated.sort_values('date_int').reset_index(drop=True)

    return df1_updated.tail(m)


def append_after_max_common_date_robust(df1: pd.DataFrame, df2: pd.DataFrame,
                                        keep_original: bool = True) -> pd.DataFrame:
    """
    增强版本：处理各种边界情况

    Args:
        df1: 原始DataFrame
        df2: 新数据DataFrame
        keep_original: 是否保留原始df1中max_common_date之前的数据

    Returns:
        合并后的DataFrame
    """
    if df1.empty or df2.empty:
        raise ValueError("错误: 输入DataFrame不能为空")

    if 'date_int' not in df1.columns or 'date_int' not in df2.columns:
        raise ValueError("错误: DataFrame必须包含date_int列")

    df1 = df1.sort_values('date_int').reset_index(drop=True)
    df2 = df2.sort_values('date_int').reset_index(drop=True)

    common_dates = set(df1['date_int']).intersection(set(df2['date_int']))

    if not common_dates:
        raise ValueError("错误: 两个DataFrame中没有相同的date_int值")

    max_common_date = max(common_dates)
    logger.info(f"最大重叠日期：{max_common_date}")

    df2_to_append = df2[df2['date_int'] >= max_common_date].copy()

    for col in df1.columns:
        if col not in df2_to_append.columns:
            if col == 'time_int':
                df2_to_append[col] = 0
            else:
                df2_to_append[col] = None

    df2_to_append = df2_to_append[df1.columns]

    if keep_original:
        df1_to_keep = df1[df1['date_int'] < max_common_date]
        result_df = pd.concat([df1_to_keep, df2_to_append], ignore_index=True)
    else:
        result_df = df2_to_append

    result_df = result_df.sort_values('date_int')
    result_df = result_df.drop_duplicates(subset='date_int', keep='last')
    result_df = result_df.reset_index(drop=True)

    return result_df


def append_and_get_tail(df1: pd.DataFrame, df2: pd.DataFrame, m: int = 20) -> pd.DataFrame:
    """追加数据并返回最后M条数据"""
    result_df = append_after_max_common_date_robust(df1, df2)
    m = min(m, len(result_df))
    return result_df.tail(m)


def generate_file_dat(df: pd.DataFrame, file_path: str) -> bool:
    """将DataFrame数据写入通达信扩展数据文件"""
    logger.info(f"生成DAT文件，数据行数: {len(df)}")
    try:
        data_to_write = []
        for _, row in df.iterrows():
            date_int = int(row['date_int'])
            time_int = int(row.get('time_int', 0))
            value_f = row['value_f']
            data_to_write.append((date_int, time_int, value_f))

        return write_binary_data(file_path, data_to_write)
    except Exception as e:
        logger.error(f"生成DAT文件错误: {e}", exc_info=True)
        return False


# def generate_file_idx(df: pd.DataFrame, file_path: str) -> bool:
#     """将DataFrame数据写入通达信扩展数据文件"""
#     logger.info(f"生成IDX文件，数据行数: {len(df)}")
#     try:
#         data_to_write = []
#         for _, row in df.iterrows():
#
#
#     except Exception as e:
#         logger.error(f"生成DAT文件错误: {e}", exc_info=True)
#         return False


def load_idx_data(idx_path: str) -> pd.DataFrame:
    """
    加载并解析idx文件，返回包含累计记录数的DataFrame

    Args:
        idx_path: idx文件路径

    Returns:
        DataFrame: 包含market_code, stock_code, record_count, cum_sum的DataFrame
    """
    logger.info(f"加载idx文件: {idx_path}")
    idx_records = parse_file_idx(idx_path)
    if not idx_records:
        logger.error(f"idx文件解析失败或为空: {idx_path}")
        return pd.DataFrame()

    # 将列表转换为DataFrame
    idx_df = pd.DataFrame(idx_records)

    # 计算累计记录数
    idx_df['cum_sum'] = idx_df['record_count'].cumsum().shift(1).fillna(0).astype(int)
    return idx_df


def load_dat_data(dat_path: str, idx_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    根据idx文件信息加载dat文件数据

    Args:
        dat_path: dat文件路径
        idx_df: 包含cum_sum和record_count的idx DataFrame

    Returns:
        Dict: 键为股票代码，值为该股票数据的DataFrame
    """
    logger.info(f"加载dat文件: {dat_path}")
    stock_data = {}

    for _, row in idx_df.iterrows():
        stock_code = row['stock_code']
        start_index = int(row['cum_sum'])
        record_count = int(row['record_count'])

        logger.debug(f"加载股票 {stock_code} 的数据，起始位置: {start_index}, 记录数: {record_count}")

        # 读取该股票的数据
        data_records = parse_file_dat2(dat_path, start_index, record_count)
        if data_records:
            stock_df = pd.DataFrame(data_records)
            stock_data[stock_code] = stock_df
        else:
            logger.warning(f"股票 {stock_code} 无数据记录")
            stock_data[stock_code] = pd.DataFrame()

    return stock_data


def merge_stock_data(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """
    合并同一股票的旧数据和新数据，保留最新的500条记录

    Args:
        old_data: 旧数据DataFrame
        new_data: 新数据DataFrame

    Returns:
        DataFrame: 合并后的数据，最多500条记录
    """
    if old_data.empty:
        return new_data.tail(500) if len(new_data) > 500 else new_data

    if new_data.empty:
        return old_data.tail(500) if len(old_data) > 500 else old_data

    # 合并数据
    merged_df = pd.concat([old_data, new_data], ignore_index=True)

    # 按日期排序并去重（保留最新的记录）
    merged_df = merged_df.sort_values('date_int')
    merged_df = merged_df.drop_duplicates(subset='date_int', keep='last')

    # 保留最新的500条记录
    if len(merged_df) > 500:
        merged_df = merged_df.tail(500)

    return merged_df.sort_values('date_int').reset_index(drop=True)


def process_incremental_update_files_optimized(
        old_idx_path: str,
        old_dat_path: str,
        new_idx_path: str,
        new_dat_path: str,
        output_idx_path: str,
        output_dat_path: str
) -> bool:
    """
    优化版的增量更新处理主函数
    """
    try:
        # 1. 并行加载idx文件
        logger.info("并行加载idx文件...")
        old_idx_df, new_idx_df = load_idx_data_parallel(old_idx_path, new_idx_path)

        if old_idx_df.empty or new_idx_df.empty:
            return False

        # 2. 确定需要加载的股票代码
        old_stocks = set(old_idx_df['stock_code'])
        new_stocks = set(new_idx_df['stock_code'])
        all_stocks = old_stocks | new_stocks

        # 3. 并行加载dat文件
        logger.info("并行加载dat文件...")
        old_dat_data, new_dat_data = load_dat_data_parallel(
            old_dat_path, old_idx_df, old_stocks,
            new_dat_path, new_idx_df, new_stocks
        )

        # 4. 处理增量更新
        updated_idx_df, updated_dat_data = process_incremental_update_optimized(
            old_idx_df, old_dat_data, new_idx_df, new_dat_data
        )

        if updated_idx_df.empty or not updated_dat_data:
            return False

        # 5. 生成新文件
        logger.info("生成新文件...")
        success = generate_files_parallel(updated_idx_df, updated_dat_data, output_idx_path, output_dat_path)

        return success

    except Exception as e:
        logger.error(f"处理增量更新时发生错误: {e}", exc_info=True)
        return False


def load_idx_data_parallel(old_path: str, new_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """并行加载idx文件"""
    # 这里可以使用多线程，但为了简单起见，先顺序执行
    old_idx_df = load_idx_data(old_path)
    new_idx_df = load_idx_data(new_path)
    return old_idx_df, new_idx_df


def load_dat_data_parallel(
        old_dat_path: str, old_idx_df: pd.DataFrame, old_stocks: Set[str],
        new_dat_path: str, new_idx_df: pd.DataFrame, new_stocks: Set[str]
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """并行加载dat文件"""
    old_dat_data = load_stock_data_optimized(old_dat_path, old_idx_df, old_stocks)
    new_dat_data = load_stock_data_optimized(new_dat_path, new_idx_df, new_stocks)
    return old_dat_data, new_dat_data


def generate_files_parallel(
        idx_df: pd.DataFrame,
        dat_data: Dict[str, pd.DataFrame],
        idx_path: str,
        dat_path: str
) -> bool:
    """并行生成文件"""
    # 先生成dat文件，再生成idx文件
    dat_success = generate_dat_file(dat_data, dat_path)
    if not dat_success:
        return False

    idx_success = generate_idx_file(idx_df, idx_path)
    return idx_success


def process_incremental_update_optimized(
        old_idx_df: pd.DataFrame,
        old_dat_data: Dict[str, pd.DataFrame],
        new_idx_df: pd.DataFrame,
        new_dat_data: Dict[str, pd.DataFrame]
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    优化版的增量更新处理

    Args:
        old_idx_df: 现有idx数据的DataFrame
        old_dat_data: 现有dat数据的字典（股票代码->DataFrame）
        new_idx_df: 增量idx数据的DataFrame
        new_dat_data: 增量dat数据的字典（股票代码->DataFrame）

    Returns:
        Tuple: (更新后的idx DataFrame, 更新后的股票数据字典)
    """
    # 使用字典快速查找
    old_idx_dict = {row['stock_code']: row for _, row in old_idx_df.iterrows()}
    new_idx_dict = {row['stock_code']: row for _, row in new_idx_df.iterrows()}

    # 获取所有股票代码
    all_stocks = sorted(set(old_idx_dict.keys()) | set(new_idx_dict.keys()))

    updated_dat_data = {}
    updated_idx_records = []
    current_cum_sum = 0

    logger.info(f"开始处理 {len(all_stocks)} 只股票的数据...")

    for stock_code in tqdm(all_stocks, desc="处理股票数据", unit="只"):
        # 确定数据来源
        if stock_code in old_idx_dict:
            market_code = old_idx_dict[stock_code]['market_code']
            source = 'old'
        else:
            market_code = new_idx_dict[stock_code]['market_code']
            source = 'new'

        # 获取数据
        old_data = old_dat_data.get(stock_code, pd.DataFrame())
        new_data = new_dat_data.get(stock_code, pd.DataFrame())

        # 根据来源处理数据
        if source == 'old' and not new_data.empty:
            # 合并现有和增量数据
            merged_data = merge_stock_data_robust(old_data, new_data)
        elif source == 'new':
            # 新增股票，直接使用增量数据
            merged_data = new_data.tail(500) if len(new_data) > 500 else new_data
        else:
            # 现有股票无增量数据
            merged_data = old_data.tail(500) if len(old_data) > 500 else old_data

        # 确保不超过500条
        if len(merged_data) > 500:
            merged_data = merged_data.tail(500)

        updated_dat_data[stock_code] = merged_data

        # 更新idx记录
        record_count = len(merged_data)
        updated_idx_records.append({
            'market_code': market_code,
            'stock_code': stock_code,
            'record_count': record_count,
            'cum_sum': current_cum_sum
        })

        current_cum_sum += record_count

    # 批量创建DataFrame
    updated_idx_df = pd.DataFrame(updated_idx_records)

    return updated_idx_df, updated_dat_data


def merge_stock_data_fast(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """
    快速合并股票数据

    Args:
        old_data: 旧数据DataFrame
        new_data: 新数据DataFrame

    Returns:
        DataFrame: 合并后的数据，最多500条记录
    """
    if old_data.empty:
        return new_data.tail(500) if len(new_data) > 500 else new_data

    if new_data.empty:
        return old_data.tail(500) if len(old_data) > 500 else old_data

    # 使用concat和drop_duplicates的优化版本
    # 先处理新数据，再处理旧数据，这样drop_duplicates会保留新数据
    merged_df = pd.concat([old_data, new_data], ignore_index=True)

    # 使用更高效的去重方法
    merged_df = merged_df.sort_values('date_int')
    merged_df = merged_df.drop_duplicates(subset='date_int', keep='last')

    # 保留最新的500条
    return merged_df.tail(500).sort_values('date_int').reset_index(drop=True)


def merge_stock_data_robust(old_data: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """
    使用 append_after_max_common_date_robust 的逻辑合并股票数据
    保留最新的500条记录

    Args:
        old_data: 旧数据DataFrame
        new_data: 新数据DataFrame

    Returns:
        DataFrame: 合并后的数据，最多500条记录
    """
    if old_data.empty:
        return new_data.tail(500) if len(new_data) > 500 else new_data

    if new_data.empty:
        return old_data.tail(500) if len(old_data) > 500 else old_data

    try:
        # 使用 append_after_max_common_date_robust 的逻辑合并数据
        merged_df = append_after_max_common_date_robust(old_data, new_data, keep_original=True)

        # 确保不超过500条记录
        if len(merged_df) > 500:
            merged_df = merged_df.tail(500)

        return merged_df

    except Exception as e:
        logger.warning(f"使用robust方法合并数据失败: {e}, 回退到快速合并方法")
        # 如果robust方法失败，回退到快速合并方法
        return merge_stock_data_fast(old_data, new_data)

def generate_dat_file(dat_data: Dict[str, pd.DataFrame], output_path: str) -> bool:
    """
    根据股票数据生成dat文件

    Args:
        dat_data: 股票数据字典（股票代码->DataFrame）
        output_path: 输出dat文件路径

    Returns:
        bool: 是否成功生成文件
    """
    logger.info(f"生成dat文件: {output_path}")

    # 收集所有数据记录
    all_records = []
    total_records = sum(len(df) for df in dat_data.values())

    # 添加进度条
    pbar = tqdm(total=total_records, desc="写入DAT记录", unit="条")

    for stock_code, df in dat_data.items():
        for _, row in df.iterrows():
            all_records.append((
                int(row['date_int']),
                int(row.get('time_int', 0)),
                float(row['value_f'])
            ))
            pbar.update(1)
    pbar.close()

    # 写入文件
    return write_binary_data(output_path, all_records)


def generate_idx_file(idx_df: pd.DataFrame, output_path: str) -> bool:
    """
    根据idx DataFrame生成idx文件

    Args:
        idx_df: 包含market_code, stock_code, record_count的DataFrame
        output_path: 输出idx文件路径

    Returns:
        bool: 是否成功生成文件
    """
    logger.info(f"生成idx文件: {output_path}")

    try:
        with open(output_path, 'wb') as f:
            for _, row in idx_df.iterrows():
                # 打包idx记录: market_code (2字节), stock_code (7字节), 保留16字节, record_count (4字节)
                stock_code_bytes = row['stock_code'].encode('gb2312').ljust(7, b'\x00')
                packed_data = struct.pack(
                    '<H7s16xI',
                    row['market_code'],
                    stock_code_bytes,
                    row['record_count']
                )
                f.write(packed_data)

        logger.info(f"成功写入idx文件: {output_path}")
        return True
    except Exception as e:
        logger.error(f"写入idx文件失败: {e}")
        return False


def merge_idx_data(old_idx_df: pd.DataFrame, new_idx_df: pd.DataFrame) -> pd.DataFrame:
    """
    高效合并现有idx数据和增量idx数据，处理股票代码不一致的情况

    Args:
        old_idx_df: 现有idx数据的DataFrame
        new_idx_df: 增量idx数据的DataFrame

    Returns:
        DataFrame: 合并后的idx数据，包含所有股票代码并按股票代码升序排列
    """
    logger.info("开始高效合并idx数据...")

    # 使用字典来快速查找记录，避免重复的DataFrame查询
    old_records_dict = {row['stock_code']: row for _, row in old_idx_df.iterrows()}
    new_records_dict = {row['stock_code']: row for _, row in new_idx_df.iterrows()}

    # 获取所有股票代码并排序
    all_stocks = sorted(set(old_records_dict.keys()) | set(new_records_dict.keys()))

    logger.info(
        f"现有股票数量: {len(old_records_dict)}, 增量股票数量: {len(new_records_dict)}, 合并后股票数量: {len(all_stocks)}")

    # 使用列表推导式高效构建合并记录
    merged_idx_records = []

    # 使用 tqdm 包装所有股票代码的迭代过程
    for stock_code in tqdm(all_stocks, desc="合并股票代码", unit="只"):  # 添加进度条
        # 优先使用现有记录，如果没有则使用增量记录
        if stock_code in old_records_dict:
            record = old_records_dict[stock_code]
            source = 'old'
        else:
            record = new_records_dict[stock_code]
            source = 'new'

        merged_idx_records.append({
            'market_code': record['market_code'],
            'stock_code': stock_code,
            'record_count': record['record_count'],
            'source': source
        })

    # 直接创建DataFrame，避免多次转换
    merged_idx_df = pd.DataFrame(merged_idx_records)

    # 计算累计记录数
    merged_idx_df['cum_sum'] = merged_idx_df['record_count'].cumsum().shift(1).fillna(0).astype(int)

    return merged_idx_df


def load_stock_data_optimized(dat_path: str, idx_df: pd.DataFrame, stock_codes: Set[str] = None) -> Dict[
    str, pd.DataFrame]:
    """
    优化版的数据加载方法

    Args:
        dat_path: dat文件路径
        idx_df: 包含cum_sum和record_count的idx DataFrame
        stock_codes: 需要加载的股票代码集合

    Returns:
        Dict: 键为股票代码，值为该股票数据的DataFrame
    """
    logger.info(f"优化加载dat文件: {dat_path}")

    if stock_codes is None:
        stock_codes = set(idx_df['stock_code'])

    # 预先计算需要读取的数据范围
    read_ranges = []
    stock_mapping = {}

    for _, row in tqdm(idx_df.iterrows(), total=len(idx_df), desc="加载股票数据", unit="只"):
        stock_code = row['stock_code']
        if stock_code not in stock_codes:
            continue

        start_index = int(row['cum_sum'])
        record_count = int(row['record_count'])
        end_index = start_index + record_count

        read_ranges.append((start_index, end_index, stock_code))
        stock_mapping[stock_code] = (start_index, record_count)

    # 按起始位置排序，减少磁盘寻址时间
    read_ranges.sort(key=lambda x: x[0])

    stock_data = {}

    # 批量读取数据
    try:
        file_size = os.path.getsize(dat_path)
        record_size = DAT_RECORD_SIZE

        with open(dat_path, 'rb') as f:
            for start_index, end_index, stock_code in read_ranges:
                # 定位到起始位置
                f.seek(start_index * record_size)

                # 读取该股票的所有数据
                data_records = []
                for _ in range(end_index - start_index):
                    data = f.read(record_size)
                    if len(data) < record_size:
                        break

                    try:
                        parsed_data = struct.unpack('<IIf', data)
                        data_records.append({
                            'date_int': parsed_data[0],
                            'time_int': parsed_data[1],
                            'value_f': parsed_data[2]
                        })
                    except struct.error:
                        break

                if data_records:
                    stock_data[stock_code] = pd.DataFrame(data_records)
                else:
                    stock_data[stock_code] = pd.DataFrame()

    except Exception as e:
        logger.error(f"读取dat文件错误: {e}")
        return {}

    return stock_data