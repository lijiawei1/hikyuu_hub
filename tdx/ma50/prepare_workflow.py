# 使用绝对导入
import os
import shutil
from datetime import datetime

import pandas as pd

from tdx.extdata_util import parse_file_idx
from tdx.ma50.preprocessor import Preprocessor
from tdx.workflow import Config, WorkflowLogger, BaseProcessor, Workflow
from tdx import extdata_util, api_utils


class PeriodProcessor(BaseProcessor):

    def process(self, data) -> (any, bool):
        self.logger.info(f"开始info文件周期更新处理，输入数据：{data}")
        # 读取info内容
        info_file_path = self.get_info_file_path()
        self.logger.debug(f"info文件路径：{info_file_path}")
        # 1.第一步，批量修改统计时间
        last_trading_day_int = data[1]
        pre_100_day_int = data[2]
        pre_300_day_int = data[3]

        # 300天
        extdata_util.write_file_info_batch(info_file_path, [10, 11], 0xA1, '<II',
                                           [pre_300_day_int, last_trading_day_int])
        # 100天
        extdata_util.write_file_info_batch(info_file_path, [12, 13, 14, 15], 0xA1, '<II',
                                           [pre_100_day_int, last_trading_day_int])

        extdata_util.write_file_info_batch(info_file_path, [16], 0xA1, '<II',
                                           [pre_300_day_int, last_trading_day_int])

        temp_extdata_path = self.get_temp_extdata_path()

        # 读取检查
        records = extdata_util.parse_file_info(info_file_path)
        df = pd.DataFrame(records)
        df.to_csv(os.path.join(temp_extdata_path, "base_extdata_info.csv"), index=False)

        # 读取df首行数据['seq']的值
        # 检查df列
        after_date_start = df[df['seq'] == 11].iloc[0]['date_start']
        if after_date_start == pre_300_day_int:
            self.logger.info("更新info文件周期成功")
            # 实际的数据加载逻辑
            return data, True
        else:
            self.logger.warning("更新info文件周期失败")
            return data, False


# 自定义处理器实现
# class DataLoader(BaseProcessor):
#     def process(self, data):
#
#         self.logger.debug(f"数据预处理：{data}")
#         self.logger.info("读取原extdata.dat文件数据...")
#
#         tdx_base_path = self.get_tdx_base_path()
#         info_file_path = self.get_info_file_path()
#
#         ma50_file = self.config.get("path", {}).get("ma50_file")
#         ma50_text = self.config.get("path", {}).get("ma50_text")
#         ma50_file_path = os.path.join(tdx_base_path, ma50_file)
#         self.logger.debug(f"info文件路径：{info_file_path}")
#         self.logger.debug(f"dat文件枯井：{ma50_file_path}")
#
#         # 实际的数据加载逻辑
#         return {"sample": "data"}


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
    workflow.add_processor(PeriodProcessor(config, logger))

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