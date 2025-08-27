# 使用绝对导入
from datetime import datetime

import pandas as pd

from tdx import extdata_util, api_utils
from tdx.workflow import BaseProcessor


class Preprocessor(BaseProcessor):
    def process(self, data):
        self.logger.info("开始前置处理，对齐日期...")
        #
        today = datetime.now().strftime("%Y-%m-%d")
        today_int = int(datetime.now().strftime("%Y%m%d"))
        last_trading_day = api_utils.APIUtils.get_nearest_trade_date(today)
        last_trading_day_int = int(last_trading_day.replace("-", ""))
        pre_100_day = api_utils.APIUtils.get_n_days_before(today, 100)
        pre_100_day_int = int(pre_100_day.replace("-", ""))
        pre_300_day = api_utils.APIUtils.get_n_days_before(today, 300)
        pre_300_day_int = int(pre_300_day.replace("-", ""))
        self.logger.debug(f"当前日期:{today}, 最近一个交易日{last_trading_day}，往前第100个交易日:{pre_100_day}，往前第300个交易日:{pre_300_day}")

        # 检查完整配置info文件的最新更新日期
        idx_list = extdata_util.parse_file_info(self.get_info_file_path())
        idx_df = pd.DataFrame(idx_list)
        # 输出 date_int列最大值
        max_date_int = idx_df['date_int'].max()
        self.logger.debug(f"完整配置的info文件最新日期：[{max_date_int}]，当前日期[{today_int}]，最近一个交易日[{last_trading_day_int}]，前100[{pre_100_day_int}]，前300[{pre_300_day_int}]")

        # 检查是否需要更新
        # if max_date_int >= last_trading_day_int:
        #     self.logger.warning("无需更新，日期对齐完成")
        #     return data, False

        self.logger.info("info文件更新日期小于最近一个交易日，需要更新统计指标！")
        # 将日期传给下一个处理器
        data = [
            today_int,
            last_trading_day_int,
            pre_100_day_int,
            pre_300_day_int,
            int(max_date_int)
        ]
        return data, True