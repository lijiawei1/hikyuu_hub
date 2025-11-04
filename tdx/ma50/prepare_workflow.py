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

        # 读取检查
        records = extdata_util.parse_file_info(info_file_path)
        info_df = pd.DataFrame(records)
        # 增加自然序号index列
        info_df['index'] = range(0, len(info_df))
        # 计算name列与index列的映射
        name_index_map = dict(zip(info_df['name'], info_df['index']))

        # 300天
        index_list = self.get_index_list(name_index_map, ['个股月多标记', '板块月多标记'])
        extdata_util.write_file_info_batch(info_file_path, index_list, 0xA0, '<bII',
                                           [1, pre_300_day_int, last_trading_day_int])
        # 100天
        index_list = self.get_index_list(name_index_map, ['上MA50标记', '全A数量标记', '新高标记', '新低标记'])
        extdata_util.write_file_info_batch(info_file_path, index_list, 0xA0, '<bII',
                                           [1, pre_100_day_int, last_trading_day_int])

        index_list = self.get_index_list(name_index_map, ['二阶段标记'])
        extdata_util.write_file_info_batch(info_file_path, index_list, 0xA0, '<bII',
                                           [1, pre_300_day_int, last_trading_day_int])

        index_list = self.get_index_list(name_index_map,
                                         ['BS个股', '动量股_D', '趋势股_D', '慢牛股_D', '动量股_W', '趋势股_W',
                                          '慢牛股_W', '动量股_M','趋势股_M', '慢牛股_M'])

        extdata_util.write_file_info_batch(info_file_path, index_list, 0xA0, '<bII',
                                           [1, pre_300_day_int, last_trading_day_int])

        # 读取检查
        records = extdata_util.parse_file_info(info_file_path)
        df = pd.DataFrame(records)
        # 文件名增加时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df.to_csv(os.path.join(self.get_temp_extdata_path(), f"base_extdata_info_{timestamp}.csv"), index=False)

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

    def get_index_list(self, name_index_map, name_list):
        # 使用name_list从name_index_map获取index列表
        index_list = [name_index_map.get(name, None) for name in name_list]
        return index_list


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