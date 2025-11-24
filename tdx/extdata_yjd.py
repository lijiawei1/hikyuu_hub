from extdata_util import *

# 读取RPS数据索引
# idx_df = load_idx_data("D:\\app\\hwx\\T0002\\extdata\\extdata_1.idx")
# print(idx_df.head())
# idx_df.to_csv("extdata_1_idx.csv")

# stock_data = load_stock_data_optimized(dat_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_1.dat",
#                                        idx_df = idx_df,
#                                        stock_codes = {'302132'})
# print(stock_data['302132'])

# 1. 加载idx文件和所有股票数据
idx_file_path = "C:\\hwx\\T0002\\extdata\\extdata_81.idx"
dat_file_path = "C:\\hwx\\T0002\\extdata\\extdata_81.dat"

# idx_df = load_idx_data(idx_file_path)
# print(idx_df.head())
# idx_df.to_csv("extdata_81_idx.csv")

# stock_data = load_stock_data_optimized(dat_path = dat_file_path,
#                                        idx_df = idx_df,
#                                        stock_codes = {'881376'})
# print(stock_data['881376'])


# 2. 一次性加载所有股票数据（注意：如果数据量大，可能会消耗较多内存）
# all_stock_data = load_dat_data(dat_file_path, idx_df)
# print(f"成功加载 {len(all_stock_data)} 只股票的数据")


# # 3. 创建空的结果列表
# result_list = []
# target_date = 20200221

# # 4. 遍历所有股票数据
# for stock_code, df in all_stock_data.items():
#     if not df.empty:
#         # 查找特定日期的数据
#         date_data = df[df['date_int'] == target_date]
        
#         if not date_data.empty:
#             value_f = date_data['value_f'].iloc[0]
#             result_list.append({
#                 'stock_code': stock_code,
#                 'value_f': value_f
#             })


# # 5. 转换为DataFrame并排序
# result_df = pd.DataFrame(result_list)
# if not result_df.empty:
#     result_df = result_df.sort_values(by='value_f', ascending=False).reset_index(drop=True)
#     print(f"\n在日期 {target_date} 找到 {len(result_df)} 条记录，按value_f倒序排序:")
#     # 打印resulf_df前50行
#     print(result_df.head(30))
# else:
#     print(f"在日期 {target_date} 没有找到任何记录")

# # 重构以上方法


def get_all_stocks_ind_by_date(
    idx_file_path: str,
    dat_file_path: str,
    target_date: int,
    output_file: Optional[str] = None,
    max_display_rows: int = 30
) -> Optional[pd.DataFrame]:
    """
    获取指定日期的所有股票数据，并按value_f倒序排序
    
    Args:
        idx_file_path: 索引文件路径
        dat_file_path: 数据文件路径
        target_date: 目标日期，格式如20200221
        output_file: 输出CSV文件路径（可选）
        max_display_rows: 最大显示行数
    
    Returns:
        排序后的DataFrame或None（如果没有找到数据）
    """
    # 1. 验证文件路径
    if not os.path.exists(idx_file_path):
        print(f"错误: 索引文件不存在: {idx_file_path}")
        return None
    
    if not os.path.exists(dat_file_path):
        print(f"错误: 数据文件不存在: {dat_file_path}")
        return None
    
    try:
        # 2. 加载索引数据
        print(f"正在加载索引文件: {idx_file_path}...")
        idx_df = load_idx_data(idx_file_path)
        
        if idx_df.empty:
            print("警告: 索引文件没有有效数据")
            return None
        
        print(f"索引文件加载完成，共包含 {len(idx_df)} 只股票")
        
        # 3. 加载所有股票数据
        print(f"正在加载所有股票数据: {dat_file_path}...")
        all_stock_data = load_dat_data(dat_file_path, idx_df)
        print(f"数据加载完成，成功加载 {len(all_stock_data)} 只股票的数据")
        
        # 4. 提取指定日期的数据
        result_list = []
        found_count = 0
        
        print(f"正在筛选日期 {target_date} 的数据...")
        for stock_code, df in all_stock_data.items():
            if not df.empty:
                # 查找特定日期的数据
                date_data = df[df['date_int'] == target_date]
                
                if not date_data.empty:
                    value_f = date_data['value_f'].iloc[0]
                    result_list.append({
                        'stock_code': stock_code,
                        'value_f': value_f
                    })
                    found_count += 1
        
        # 5. 转换为DataFrame并排序
        if result_list:
            result_df = pd.DataFrame(result_list)
            # 按value_f倒序排序
            result_df = result_df.sort_values(by='value_f', ascending=False).reset_index(drop=True)
            
            print(f"\n在日期 {target_date} 找到 {len(result_df)} 条记录，按value_f倒序排序:")
            # 打印前N行
            display_rows = min(max_display_rows, len(result_df))
            print(result_df.head(display_rows))
            
            # 保存到CSV（如果指定）
            if output_file:
                try:
                    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                    print(f"\n结果已保存到: {output_file}")
                except Exception as e:
                    print(f"警告: 保存文件失败: {e}")
            
            return result_df
        else:
            print(f"在日期 {target_date} 没有找到任何记录")
            return None
            
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return None




# 使用示例
if __name__ == "__main__":
    # 配置参数
    IDX_FILE_PATH = "C:\\hwx\\T0002\\extdata\\extdata_81.idx"
    DAT_FILE_PATH = "C:\\hwx\\T0002\\extdata\\extdata_81.dat"
    TARGET_DATE = 20200221
    OUTPUT_FILE = f"stock_data_{TARGET_DATE}_sorted.csv"
    
    # 执行主函数
    result_df = get_all_stocks_ind_by_date(
        idx_file_path=IDX_FILE_PATH,
        dat_file_path=DAT_FILE_PATH,
        target_date=TARGET_DATE,
        output_file=OUTPUT_FILE,
        max_display_rows=30
    )

    print(result_df.head(50))

    # value_f 是成交额占比,计算前5%的板块成交额占比总和
    # top_5_percent = result_df.head(int(len(result_df) * 0.05))
    # top_5_percent_value_f_sum = top_5_percent['value_f'].sum()
    # print(f"前5%板块成交额占比总和: {top_5_percent_value_f_sum:.4f}")

    # 求和 value_f,验证是否100%
    # total_value_f = result_df['value_f'].sum()
    # print(f"所有板块成交额占比总和: {total_value_f:.4f}")

    


# records = parse_file_dat("D:\\app\\hwx\\T0002\\extdata\\extdata_11.dat")
# df = pd.DataFrame(records)
# print(df)



# write_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', create=datetime.now())
# 1.第一步，批量修改统计时间
# write_file_info_batch('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', [12, 13], 0xA1, '<II', [20230822, 20240820])
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak')

# read_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata111.info.bak')
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info')

# 2.第二步，解析
# parse_file_idx("D:\\app\\hwx\\T0002\\extdata\\extdata_13.idx")

# records = parse_file_dat("D:\\app\\hwx\\T0002\\extdata\\extdata_11.dat")
# df = pd.DataFrame(records)
#
# result_df = df.groupby('date_int')['value_f'].sum().reset_index()
# result_df.to_csv("extdata_11_work.csv")
#
# records = parse_file_dat("F:\\gfzq_tdx\\T0002\\extdata\\extdata_11.dat")
# df = pd.DataFrame(records)
# # 统计 MA50数量，同样是df
# result_df = df.groupby('date_int')['value_f'].sum().reset_index()
# # print("分组统计后的DataFrame:")
# result_df.to_csv("extdata_11_base.csv")


#
# records = parse_file_idx("data/extdata_68.idx")
# df = pd.DataFrame(records)
# print(df)

# records = parse_file_dat("data/extdata_68.dat")
# df = pd.DataFrame(records)
# print(df)
#
# # after_df = update_and_append_df(result_df, df, 7, 20)
# after_df = append_and_get_tail(df, result_df,500)
# after_df.to_csv("temp.csv")
# print(after_df)
#
#
# # 最后一步：重新生成dat文件
# generate_file_dat(after_df, "D:\\app\\hwx\\T0002\\extdata\\extdata_63_new.dat")
#
#
#
# records = parse_file_dat("D:\\app\\hwx\\T0002\\extdata\\extdata_63_new.dat")
# df = pd.DataFrame(records)
# print(df)
