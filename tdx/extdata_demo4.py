from extdata_util import *
import os

from tdx import api_utils


def process_multiple_stocks(idx_file_path: str, dat_file_path: str, stock_codes: list,
                            output_dir: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    批量处理多个股票代码

    Args:
        idx_file_path: idx文件路径
        dat_file_path: dat文件路径
        stock_codes: 股票代码列表
        output_dir: 输出目录，如果为None则使用当前目录

    Returns:
        Dict: 键为股票代码，值为对应的DataFrame
    """
    # 先解析idx文件一次，避免重复解析
    records = parse_file_idx(idx_file_path)
    if not records:
        print(f"错误: 无法解析idx文件 {idx_file_path}")
        return {}

    idx_df = pd.DataFrame(records)
    idx_df['cum_sum'] = idx_df['record_count'].cumsum().shift(1).fillna(0).astype(int)

    results = {}

    for stock_code in stock_codes:
        print(f"\n处理股票 {stock_code}...")

        # 查找股票代码
        stock_row = idx_df[idx_df['stock_code'] == stock_code]
        if stock_row.empty:
            print(f"警告: 跳过不存在的股票代码 {stock_code}")
            continue

        # 获取股票信息
        row = stock_row.iloc[0]
        record_count = int(row['record_count'])
        cum_sum = int(row['cum_sum'])

        print(f"stock_code={stock_code}, record_count={record_count}, start_index={cum_sum}")

        # 解析dat文件
        records = parse_file_dat2(dat_file_path, cum_sum, record_count)
        if not records:
            print(f"警告: 股票 {stock_code} 没有数据记录")
            results[stock_code] = pd.DataFrame()
            continue

        df = pd.DataFrame(records)
        results[stock_code] = df

        # 保存CSV
        if output_dir:
            csv_path = f"{output_dir}/extdata_42_dat_{stock_code}.csv"
        else:
            csv_path = f"extdata_42_dat_{stock_code}.csv"

        try:
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"已保存: {csv_path}")
        except Exception as e:
            print(f"保存失败: {e}")

    return results



def compare_dataframes_with_precision(df1: pd.DataFrame, df2: pd.DataFrame,
                                      date_col: str = 'date_int', value_col: str = 'value_f',
                                      precision: float = 1e-6) -> Dict[str, any]:
    """
    比较两个DataFrame，支持浮点数精度比较

    Args:
        df1: 第一个DataFrame
        df2: 第二个DataFrame
        date_col: 日期列名
        value_col: 值列名
        precision: 浮点数比较精度

    Returns:
        Dict: 包含比较结果的字典
    """
    if df1.empty or df2.empty:
        return {
            'intersection_size': 0,
            'different_values_count': 0,
            'different_values_ratio': 0.0,
            'df1_only_count': len(df1) if not df1.empty else 0,
            'df2_only_count': len(df2) if not df2.empty else 0
        }

    # 合并DataFrame
    merged = pd.merge(df1, df2, on=date_col, how='outer',
                      suffixes=('_df1', '_df2'), indicator=True)

    df1_only = merged[merged['_merge'] == 'left_only']
    df2_only = merged[merged['_merge'] == 'right_only']
    both_exist = merged[merged['_merge'] == 'both']

    # 使用numpy的isclose进行浮点数精度比较
    values_equal = np.isclose(
        both_exist[f'{value_col}_df1'].values,
        both_exist[f'{value_col}_df2'].values,
        rtol=precision, atol=precision
    )

    different_count = len(both_exist) - np.sum(values_equal)

    intersection_size = len(both_exist)
    if intersection_size > 0:
        different_ratio = different_count / intersection_size
    else:
        different_ratio = 0.0

    # 获取差异详情
    different_mask = ~values_equal
    different_details = both_exist.iloc[different_mask][[date_col, f'{value_col}_df1', f'{value_col}_df2']]

    return {
        'intersection_size': intersection_size,
        'different_values_count': different_count,
        'different_values_ratio': different_ratio,
        'same_values_count': np.sum(values_equal),
        'df1_only_count': len(df1_only),
        'df2_only_count': len(df2_only),
        'df1_total': len(df1),
        'df2_total': len(df2),
        'different_details': different_details
    }


import pandas as pd
from typing import Tuple, Dict


def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, date_col: str = 'date_int', value_col: str = 'value_f') -> \
Dict[str, any]:
    """
    比较两个DataFrame，统计date_int相同的行中value_f值的差异

    Args:
        df1: 第一个DataFrame
        df2: 第二个DataFrame
        date_col: 日期列名
        value_col: 值列名

    Returns:
        Dict: 包含比较结果的字典
    """
    # 确保DataFrame不为空
    if df1.empty or df2.empty:
        return {
            'intersection_size': 0,
            'different_values_count': 0,
            'different_values_ratio': 0.0,
            'df1_only_count': len(df1) if not df1.empty else 0,
            'df2_only_count': len(df2) if not df2.empty else 0,
            'total_comparable': 0
        }

    # 合并两个DataFrame来比较
    merged = pd.merge(df1, df2, on=date_col, how='outer',
                      suffixes=('_df1', '_df2'), indicator=True)

    # 统计各种情况
    df1_only = merged[merged['_merge'] == 'left_only']
    df2_only = merged[merged['_merge'] == 'right_only']
    both_exist = merged[merged['_merge'] == 'both']

    # 计算值不同的行
    different_values = both_exist[both_exist[f'{value_col}_df1'] != both_exist[f'{value_col}_df2']]

    # 计算值相同的行
    same_values = both_exist[both_exist[f'{value_col}_df1'] == both_exist[f'{value_col}_df2']]

    # 计算结果
    intersection_size = len(both_exist)
    different_count = len(different_values)

    if intersection_size > 0:
        different_ratio = different_count / intersection_size
    else:
        different_ratio = 0.0

    return {
        'intersection_size': intersection_size,  # 交集大小（相同日期的行数）
        'different_values_count': different_count,  # 值不同的行数
        'different_values_ratio': different_ratio,  # 值不同的比率
        'same_values_count': len(same_values),  # 值相同的行数
        'df1_only_count': len(df1_only),  # 只在df1中存在的行数
        'df2_only_count': len(df2_only),  # 只在df2中存在的行数
        'df1_total': len(df1),  # df1总行数
        'df2_total': len(df2),  # df2总行数
        'different_details': different_values[[date_col, f'{value_col}_df1', f'{value_col}_df2']]  # 差异详情
    }


def print_comparison_result(result: Dict[str, any]) -> None:
    """
    打印比较结果

    Args:
        result: 比较结果字典
    """
    print("=" * 60)
    print("DataFrame 比较结果")
    print("=" * 60)
    print(f"交集大小 (相同日期的行数): {result['intersection_size']}")
    print(f"值不同的行数: {result['different_values_count']}")
    print(f"值不同的比率: {result['different_values_ratio']:.2%}")
    print(f"值相同的行数: {result['same_values_count']}")
    print(f"只在第一个DF中的行数: {result['df1_only_count']}")
    print(f"只在第二个DF中的行数: {result['df2_only_count']}")
    print(f"第一个DF总行数: {result['df1_total']}")
    print(f"第二个DF总行数: {result['df2_total']}")

    # 打印差异详情
    if not result['different_details'].empty:
        print("\n值不同的行详情:")
        print(result['different_details'].to_string(index=False))


def compare_extdata_stocks(df1: pd.DataFrame, df2: pd.DataFrame,
                           df1_name: str = "DF1", df2_name: str = "DF2") -> Dict[str, any]:
    """
    专门用于比较通达信扩展数据格式的DataFrame

    Args:
        df1: 第一个DataFrame
        df2: 第二个DataFrame
        df1_name: 第一个DF的名称（用于显示）
        df2_name: 第二个DF的名称（用于显示）

    Returns:
        Dict: 比较结果
    """
    print(f"比较 {df1_name} 和 {df2_name}:")
    print(f"{df1_name} 行数: {len(df1)}")
    print(f"{df2_name} 行数: {len(df2)}")

    result = compare_dataframes(df1, df2)
    print_comparison_result(result)

    return result



# 批量处理示例
if __name__ == "__main__":
    idx_file_path = "F:\\stock\\temp_tdx_extdata\\20250827\\extdata_42.idx"
    dat_file_path = "F:\\stock\\temp_tdx_extdata\\20250827\\extdata_42.dat"
    stock_codes = ["873527"]
    output_dir = "output_data_temp"
    os.makedirs(output_dir, exist_ok=True)
    temp_results = process_multiple_stocks(idx_file_path, dat_file_path, stock_codes, output_dir)
    print(f"\n 环境temp处理完成，成功处理 {len(temp_results)} 只股票")

    idx_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx"
    dat_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat"
    output_dir = "output_data_work"
    os.makedirs(output_dir, exist_ok=True)
    work_results = process_multiple_stocks(idx_file_path, dat_file_path, stock_codes, output_dir)
    print(f"\n 环境work处理完成，成功处理 {len(work_results)} 只股票")

    idx_file_path = "F:\\gfzq_tdx\\T0002\\extdata\\extdata_42.idx"
    dat_file_path = "F:\\gfzq_tdx\\T0002\\extdata\\extdata_42.dat"
    output_dir = "output_data_base"
    os.makedirs(output_dir, exist_ok=True)
    base_results = process_multiple_stocks(idx_file_path, dat_file_path, stock_codes, output_dir)
    print(f"\n 环境base处理完成，成功处理 {len(work_results)} 只股票")

    # temp_df = temp_results[stock_codes[0]]
    work_df = work_results[stock_codes[0]]
    base_df = base_results[stock_codes[0]]
    temp_df = temp_results[stock_codes[0]]

    # compare_extdata_stocks(base_df, work_df, "base", "work")
    compare_extdata_stocks(temp_df, work_df, "temp", "work")



    print(api_utils.APIUtils.get_n_days_before("2025-08-22", 50))
