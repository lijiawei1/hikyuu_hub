from extdata_util import *

# write_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', create=datetime.now())
# 1.第一步，批量修改统计时间
# write_file_info_batch('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', [12, 13], 0xA1, '<II', [20230822, 20240820])
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak')

# read_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata111.info.bak')
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info')

# 2.第二步，解析
"""
records = parse_file_idx("D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx")
df = pd.DataFrame(records)
df['cum_sum'] = df['record_count'].cumsum().shift(1).fillna(0)
df.to_csv("extdata_42_idx_work.csv")

# 从df中取得 stock_code 列为 873706 的i和 value_f的值
# stock_code = '873706'
stock_code = '000548'
row = df[df['stock_code'] == stock_code].iloc[0]
index = int(row['i'])
record_count = (row['record_count'])
cum_sum = int(row['cum_sum'])

# 需要计算stock_code之前的所有record_count的累加值
print(f"stock_code={stock_code}, index={index}, record_count={record_count}, start_index={cum_sum}")

# 定位 dat文件的记录
records = parse_file_dat2("D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat", cum_sum, record_count)
df = pd.DataFrame(records)
df.to_csv(f"extdata_42_dat_work_{stock_code}.csv")
"""

# idf_df = load_idx_data("D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx")
# idf_df.to_csv("extdata_42_idx_work.csv")

# mapping = load_dat_data("D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat", idf_df)
# print(len(mapping.keys()))

# idf_df = load_idx_data("D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx")
# idf_df.to_csv("extdata_42_idx_work.csv")

def load_dat_data(file_path: str, stock_code: str, df: pd.DataFrame) -> pd.DataFrame:
    row = df[df['stock_code'] == stock_code].iloc[0]
    index = int(row['i'])
    record_count = (row['record_count'])
    cum_sum = int(row['cum_sum'])
    print(f"stock_code={stock_code}, index={index}, record_count={record_count}, start_index={cum_sum}")
    records = parse_file_dat2(file_path, cum_sum, record_count)
    df = pd.DataFrame(records)
    return df

# idx_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx"
# dat_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat"

idx_file_path = "F:\\stock\\temp_tdx_extdata\\20250827\\extdata_42.idx"
dat_file_path = "F:\\stock\\temp_tdx_extdata\\20250827\\extdata_42.dat"

records = parse_file_idx(idx_file_path)
df = pd.DataFrame(records)
df['cum_sum'] = df['record_count'].cumsum().shift(1).fillna(0)

stock_code = "873706"

df = load_dat_data(dat_file_path, stock_code, df)
df.to_csv(f"extdata_42_dat_work_{stock_code}.csv")






# records = parse_file_dat("D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat")
# df = pd.DataFrame(records)
# df.to_csv("extdata_42_dat_work.csv")


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
# parse_file_idx("D:\\app\\hwx\\T0002\\extdata\\extdata_61.idx")
# records = parse_file_dat("D:\\app\\hwx\\T0002\\extdata\\extdata_61.dat")
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
