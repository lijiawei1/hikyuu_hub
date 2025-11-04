from extdata_util import *
from extdata_util import setup_logging

setup_logging(logging.DEBUG)

# write_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', create=datetime.now())
# 1.第一步，批量修改统计时间
# write_file_info_batch('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak', [12, 13], 0xA1, '<II', [20230822, 20240820])
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info.bak')

# read_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata111.info.bak')
# parse_file_info('D:\\app\\hwx\\T0002\\extdata\\extdata.info')

# 2.第二步，解析
# 全量idx文件：D:\app\hwx\T0002\extdata\extdata_42.idx
# 全量dat文件：D:\app\hwx\T0002\extdata\extdata_42.idx
# 增量idx文件：F:\gfzq_tdx\T0002\extdata\extdata_42.idx
# 增量dat文件：F:\gfzq_tdx\T0002\extdata\extdata_42.idx
# 临时追加idx文件：F:\stock\temp_tdx_extdata\20250927\extdata_42.idx
# 临时追加dat文件：F:\stock\temp_tdx_extdata\20250927\extdata_42.dat

work_idx_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.idx"
work_dat_file_path = "D:\\app\\hwx\\T0002\\extdata\\extdata_42.dat"
base_idx_file_path = "F:\\gfzq_tdx\\T0002\\extdata\\extdata_42.idx"
base_dat_file_path = "F:\\gfzq_tdx\\T0002\\extdata\\extdata_42.dat"
dest_idx_file_path = "F:\\stock\\temp_tdx_extdata\\20250927\\extdata_42.idx"
dest_dat_file_path = "F:\\stock\\temp_tdx_extdata\\20250927\\extdata_42.dat"

process_incremental_update_files_optimized(
    old_idx_path=work_idx_file_path,
    old_dat_path=work_dat_file_path,
    new_idx_path=base_idx_file_path,
    new_dat_path=base_dat_file_path,
    output_idx_path=dest_idx_file_path,
    output_dat_path=dest_dat_file_path,
)
