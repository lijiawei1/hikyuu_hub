from extdata_util import *

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
