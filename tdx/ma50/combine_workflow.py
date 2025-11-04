# 使用绝对导入
import os
import shutil
from doctest import debug

import pandas as pd

from tdx import extdata_util
from tdx.extdata_util import parse_file_dat
from tdx.ma50.preprocessor import Preprocessor
from tdx.workflow import Config, WorkflowLogger, BaseProcessor, Workflow


class TempDirProcessor(BaseProcessor):
    def process(self, data: any) -> (any, bool):
        # 临时目录
        temp_extdata_path = self.get_temp_extdata_path()
        self.logger.info(f"合并数据临时目录：{temp_extdata_path}")

        # 使用当前日期在临时目录下新建文件夹，作为后面使用存放临时数据，如果目录存在，则清空
        temp_dir_name = str(data[0])
        temp_dir_path = os.path.join(temp_extdata_path, temp_dir_name)
        try:
            if os.path.exists(temp_dir_path):
                temp_dir_files = os.listdir(temp_dir_path)
                for file in temp_dir_files:
                    to_delete = os.path.join(temp_dir_path, file)
                    os.remove(to_delete)
                    self.logger.debug(f"移除文件{to_delete}")
                self.logger.info(f"清理临时目录下[{len(temp_dir_files)}]个文件成功")
            else:
                os.makedirs(temp_dir_path, exist_ok=True)
                self.logger.info(f"创建临时目录：{temp_dir_path}")

            return data + [temp_dir_path], True
        except Exception as e:
            self.logger.error(f"清理临时目录失败：{e}")
            return data, False

class ApppendProcessor(BaseProcessor):
    def process(self, data):

        append_list = self.get_append_list()
        index = int(append_list[0])
        temp_dir_path = data[5]
        self.logger.info(f"开始追加数据到临时目录：{temp_dir_path}")
        work_tdx_path = self.get_work_tdx_path()
        tdx_base_path = self.get_tdx_base_path()

        # 工作通达信目录（银河海王星）
        work_idx_file_path = os.path.join(work_tdx_path, f"extdata_{index}.idx")
        work_dat_file_path = os.path.join(work_tdx_path, f"extdata_{index}.dat")
        # 基础通达信目录（广发）
        base_idx_file_path = os.path.join(tdx_base_path, f"extdata_{index}.idx")
        base_dat_file_path = os.path.join(tdx_base_path, f"extdata_{index}.dat")
        # 临时目录
        dest_idx_file_path = os.path.join(temp_dir_path, f"extdata_{index}.idx")
        dest_dat_file_path = os.path.join(temp_dir_path, f"extdata_{index}.dat")

        self.logger.debug(f"全量idx文件：{work_idx_file_path}")
        self.logger.debug(f"全量dat文件：{work_dat_file_path}")
        self.logger.debug(f"增量idx文件：{base_idx_file_path}")
        self.logger.debug(f"增量dat文件：{base_idx_file_path}")
        self.logger.debug(f"临时追加idx文件：{dest_idx_file_path}")
        self.logger.debug(f"临时追加dat文件：{dest_dat_file_path}")


        # extdata_util.process_incremental_update_files_optimized(
        #     old_idx_path=work_idx_file_path,
        #     old_dat_path=work_dat_file_path,
        #     new_idx_path=base_idx_file_path,
        #     new_dat_path=base_dat_file_path,
        #     output_idx_path=dest_idx_file_path,
        #     output_dat_path=dest_dat_file_path,
        # )


class CombineProcessor(BaseProcessor):
    def process(self, data):
        temp_dir_path = data[5]
        self.logger.info(f"开始合并数据到临时目录：{temp_dir_path}")

        flag_sum_mapping = self.get_flag_sum_mapping()
        self.logger.info(f"标记文件与统计文件映射：{flag_sum_mapping}")

        # 遍历flag_sum_mapping
        for flag_file, sum_file in flag_sum_mapping.items():

            flag_file_path = os.path.join(self.get_tdx_base_path(), flag_file)
            self.logger.debug(f"标记文件：{flag_file_path}")
            # 读取flag_file,
            flag_df = pd.DataFrame(parse_file_dat(flag_file_path))
            if len(flag_df) == 0:
                self.logger.warning(f"标记文件：{flag_file_path}无数据， 检查是否刷新扩展数据")
                continue
            # 分组统计每日MA50数量
            base_flag_df = flag_df.groupby('date_int')['value_f'].sum().reset_index()
            base_flag_df.to_csv(os.path.join(temp_dir_path, sum_file + "_base.csv"))

            work_file_path = os.path.join(self.get_work_tdx_path(), sum_file)
            self.logger.debug(f"待合并数据文件：{work_file_path}")

            work_sum_df = pd.DataFrame(parse_file_dat(work_file_path))
            work_sum_df.to_csv(os.path.join(temp_dir_path, sum_file + "_work.csv"))

            result_df = extdata_util.append_and_get_tail(work_sum_df, base_flag_df, m = 500)
            result_df.to_csv(os.path.join(temp_dir_path, sum_file + "_result.csv"))

            # self.logger.info(f"数据合并完毕，追加（覆盖）最后[{m}]天的数据，过程数据已输出csv文件")
            # 待生成
            res = extdata_util.generate_file_dat(result_df, os.path.join(temp_dir_path, sum_file))
            if res:
                self.logger.info(f"数据{sum_file}合并完成")
            else:
                self.logger.warning(f"数据{sum_file}合并失败")

        return data, True


class CopyResourceProcessor(BaseProcessor):

    def process(self, data: any) -> (any, bool):
        self.logger.info(f"开始复制资源文件")

        try:
            # 从工作TDX目录复制新的info文件到临时目录
            info_file_path = self.get_info_file_path()
            temp_dir_path = data[5]

            shutil.copy2(info_file_path, temp_dir_path)
            self.logger.debug(f"复制[{info_file_path}]到[{temp_dir_path}]")

            # 复制目录下所有后续名为idx的文件到临时目录
            work_tdx_path = self.get_work_tdx_path()
            idx_files = [f for f in os.listdir(work_tdx_path) if f.endswith('.idx')]
            for file in idx_files:
                file_path = os.path.join(work_tdx_path, file)
                shutil.copy2(file_path, temp_dir_path)
                self.logger.debug(f"复制[{file_path}]到[{temp_dir_path}]")


            # 复制指定文件名的文件到临时目录
            copy_ranges = self.get_copy_ranges()
            self.logger.debug(copy_ranges)
            for range_ele in copy_ranges:
                start, end = range_ele
                for i in range(start, end + 1):
                    file_path = os.path.join(work_tdx_path, f"extdata_{str(i)}.dat")
                    shutil.copy2(file_path, temp_dir_path)
                    self.logger.debug(f"复制[{file_path}]到目录[{temp_dir_path}]")

            # 复制剩余的指定序号的dat文件
        except FileNotFoundError as e:
            self.logger.error(f"目录不存在: {e.filename}")
        except PermissionError as e:
            self.logger.error(f"权限拒绝: {e.filename}")
        except Exception as e:
            self.logger.error(f"文件复制失败: {str(e)}")


# 使用工作流框架
def main():
    # 加载配置
    config = Config("ma50_config.yaml")

    # 初始化日志
    logger = WorkflowLogger(
        name="ma50_workflow",
        log_level=config.get("logging", {}).get("level", "DEBUG"),
        log_file=config.get("logging", {}).get("file")
    )

    # 创建工作流
    workflow = Workflow(config, logger)

    # 添加处理器
    workflow.add_processor(Preprocessor(config, logger))
    workflow.add_processor(TempDirProcessor(config, logger))
    # workflow.add_processor(CombineProcessor(config, logger))
    workflow.add_processor(ApppendProcessor(config, logger))
    # workflow.add_processor(CopyResourceProcessor(config, logger))

    try:
        # 初始化工作流
        workflow.setup()

        # 执行工作流
        result = workflow.execute()

        # 输出结果
        logger.info(f"工作流执行结果: {result}")

    finally:
        # 清理工作流
        workflow.teardown()


if __name__ == "__main__":
    main()