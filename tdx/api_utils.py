import logging
import os
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

# 配置日志
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class APIUtils:
    """
    API 工具类，包含一些常用的工具方法。
    """


    @staticmethod
    def convert_date_to_str(date):
        """
        将日期对象转换为字符串格式（YYYY-MM-DD）。
        :param date: 日期对象
        :return: 日期字符串
        """
        if isinstance(date, pd.Timestamp):
            return date.strftime('%Y-%m-%d')
        elif isinstance(date, str):
            return date
        else:
            raise ValueError("输入的日期格式不支持，请使用 pandas.Timestamp 或字符串类型。")

    @staticmethod
    def calculate_percentage_change(old_value, new_value):
        """
        计算两个值之间的百分比变化。
        :param old_value: 旧值
        :param new_value: 新值
        :return: 百分比变化
        """
        if old_value == 0:
            raise ValueError("旧值不能为零，否则无法计算百分比变化。")
        return ((new_value - old_value) / old_value) * 100

    @classmethod
    def get_previous_trading_day(cls, date):
        '''
        获取的date的前一个交易日
        :param date:
        :return:
        '''
        try:
            # 交易日期列表
            trade_dates = ak.tool_trade_date_hist_sina()
            # trade_dates的'trade_date'列是datetime类型，需要转换为字符串类型列表
            trade_dates['trade_date'] = trade_dates['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # 筛选出指定日期之前的交易日
            previous_trading_day = trade_dates[trade_dates['trade_date'] < date]['trade_date'].iloc[-1]
            # 将日期转换为字符串格式
            return previous_trading_day

        except Exception as e:
            logging.error(f"Error fetching trade dates: {str(e)}")
            return []

    @staticmethod
    def get_trade_dates_range(start_date: str, n: int):
        """
        获取从指定日期开始的n个交易日
        :param start_date: 起始日期(YYYY-MM-DD)
        :param n: 需要获取的交易日数量，正数表示往后，负数表示往前
        :return: 交易日期列表，如果出错返回空列表
        """
        try:
            trade_dates = APIUtils.get_trade_dates_from_local()
            if not trade_dates:
                logging.warning("交易日列表为空")
                return []

            # 确保交易日列表已排序
            trade_dates = sorted(trade_dates)

            # 处理start_date不在交易日列表中的情况
            if start_date not in trade_dates:
                # 使用bisect快速查找插入位置
                import bisect
                pos = bisect.bisect_left(trade_dates, start_date)
                if pos == len(trade_dates):
                    return []  # 所有日期都小于start_date
                start_index = pos
            else:
                start_index = trade_dates.index(start_date)

            # 处理边界情况
            if n >= 0:
                end_index = min(start_index + n, len(trade_dates))
                return trade_dates[start_index:end_index]
            else:
                start_index = max(0, start_index + n)  # n为负数，所以是减
                return trade_dates[start_index:start_index - n]  # -n得到正数

        except Exception as e:
            logging.error(f"获取交易日范围出错: {str(e)}", exc_info=True)
            return []



    @staticmethod
    def get_trade_dates_between(start_date, end_date):
        """
        获取指定日期范围内的所有交易日期
        :return: 交易日期列表
        """
        try:
            list = APIUtils.get_trade_dates_from_local()
            # 交易日期列表
            trade_dates = pd.DataFrame(list, columns=['trade_date'])
            trade_dates['trade_date'] = trade_dates['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # 筛选出指定时间段内的交易日
            trade_dates = trade_dates[(trade_dates['trade_date'] >= start_date) & (trade_dates['trade_date'] <= end_date)]['trade_date'].tolist()
            # 将日期转换为字符串格式
            return trade_dates
        except Exception as e:
            logging.error(f"Error fetching trade dates: {str(e)}")
            return []

    @staticmethod
    def get_trade_dates_from_local():
        """
        从txt文件中读入交易日期列表
        :return: 交易日期列表
        """
        try:
            # 获取项目根目录
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            # 拼接完整文件路径
            file_path = os.path.join(project_root, 'tdx', 'trade_dates.txt')

            with open(file_path, 'r') as f:
                trade_dates = f.read().splitlines()
            return trade_dates
        except Exception as e:
            logging.error(f"Error reading trade dates from file: {str(e)}")
            return []

    @staticmethod
    def get_nearest_trade_date(target_date:str):
        """
        获取指定日期的最近一个交易日(包含指定日期)
        :param date: 日期，格式为 YYYY-MM-DD
        :return: 最近一个交易日的日期，如果出错返回None
        """
        try:
            # 交易日期列表
            trade_dates = APIUtils.get_trade_dates_from_local()
            #trade_dates的'trade_date'列是str类型
            # 筛选出少于等于指定日期的交易日
            import bisect
            index = bisect.bisect_left(trade_dates, target_date)
            # 处理三种情况：
            # 1. 日期在列表末尾之后
            # 2. 日期精确匹配
            # 3. 日期在列表中间但不存在
            if index >= len(trade_dates):
                return trade_dates[-1]
            elif trade_dates[index] == target_date:
                return target_date
            else:
                return trade_dates[index - 1] if index > 0 else None

        except Exception as e:
            logging.error(f"获取指定日期{date}的最近一个交易日出错: {str(e)}", exc_info=True)
            return None

    @staticmethod
    def get_n_days_before(date: str, n: int):
        """
        从日期列表中获取指定date开始往前数第n个交易日的日期
        先定位date在日期列表中的位置，然后往前数n个交易日
        :param date: 日期，格式为 YYYY-MM-DD
        :param n: 往后的交易日数量，必须为正整数
        :return: 往后第n个交易日的日期，如果出错返回None
        """
        try:
            # 检查n是否为正整数
            if not isinstance(n, int) or n <= 0:
                logging.warning("n必须为正整数")
                return None

            # 获取交易日列表
            trade_dates = APIUtils.get_trade_dates_from_local()
            if not trade_dates:
                logging.warning("交易日列表为空")
                return None

            # 确保交易日列表已排序
            trade_dates = sorted(trade_dates)

            # 找到date在交易日列表中的位置
            import bisect
            date_index = bisect.bisect_left(trade_dates, date)

            # 计算目标索引：往后数第n个交易日，即date_index + n -1
            target_index = date_index - n + 1

            # 检查目标索引是否超出范围
            if target_index >= len(trade_dates):
                logging.warning(f"无法获取往后第{n}个交易日，超出交易日列表范围")
                return None

            return trade_dates[target_index]

        except Exception as e:
            logging.error(f"获取{date}往后第{n}个交易日的日期出错: {str(e)}", exc_info=True)
            return None



    @staticmethod
    def get_trade_dates(latest_date: str, default_date: str = None):
        """
        latest_date 是数据库查询到的最新交易日期，可以是某种数据已经更新的最新日期，可能为空，若不为空，则肯定是交易日期
        default_date 是默认的日期，用于处理latest_date为None的情况
        现在需要返回从latest_date到今天的所有交易日期
        :param latest_date: 已经处理的最近交易日期，格式为 YYYY-MM-DD
        :param default_date: latest_date为空时的默认值，默认昨天，格式为 YYYY-MM-DD
        :return:
        """
        try:
            today = datetime.today().strftime('%Y-%m-%d')
            # 交易日期列表
            trade_dates = ak.tool_trade_date_hist_sina()
            if default_date is None:
                # 默认昨天
                default_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


            if latest_date is None:
                latest_date = default_date

            # trade_dates的'trade_date'列是datetime类型，需要转换为字符串类型列表
            trade_dates['trade_date'] = trade_dates['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            # 筛选出指定时间段内的交易日
            trade_dates = \
            trade_dates[(trade_dates['trade_date'] > latest_date) & (trade_dates['trade_date'] <= today)][
                'trade_date'].tolist()

            # 将日期转换为字符串格式
            return trade_dates
        except Exception as e:
            logging.error(f"Error fetching trade dates: {str(e)}")
            return []

    @classmethod
    def get_xls_files(cls, ths_limit_up_dir):
        """
        获取指定目录下的所有xls文件，文件名 limit_up_analysis_2024-11-04.xls
        :param ths_limit_up_dir:
        :return: 文件名列表
        """
        # 获取目录下所有xls文件
        xls_files = [f for f in os.listdir(ths_limit_up_dir) if f.endswith('.xls')]
        return xls_files


    @staticmethod
    def print_daily_replay_data(data):
        """
        以一行一个字段的格式输出 DailyReplayData 对象的字段值

        :param data: DailyReplayData 类的实例
        """
        fields = [
            'date', 'volume', 'amount', 'emotion_index', 'up_limit_count', 'up_7_count',
            'up_5_count', 'up_2_count', 'up_count', 'no_change_count', 'down_limit_count',
            'down_7_count', 'down_5_count', 'down_2_count', 'down_count', 'amplitude_10_count',
            'amplitude_5_count', 'amplitude_2_count', 'turnover_5_count', 'turnover_2_count',
            'board_success_rate', 'board_profit_rate', 'board_1_success_rate', 'board_1_profit_rate',
            'board_2_success_rate', 'board_2_profit_rate', 'board_3_success_rate', 'board_3_profit_rate',
            'board_4_success_rate', 'board_4_profit_rate', 'up_limit_1_count', 'up_limit_2_count',
            'up_limit_3_count', 'up_limit_more_count', 'up_limit_2_rate', 'up_limit_3_rate',
            'up_limit_more_rate', 'up_limit_height', 'break_board_rate',
            'up_limit_today_avg_change_percent', 'break_board_today_avg_change_percent', 'drawdown_count'
        ]
        for field in fields:
            value = getattr(data, field)
            logging.info(f"{field}: {value}")

    @classmethod
    def convert_list_to_df(cls, list, model_class):
        """
        将列表转换为DataFrame
        :param list:
        :return:
        """
        # 转换为DataFrame
        data = [
            {
                column.name: getattr(quote, column.name)
                for column in model_class.__table__.columns
            }
            for quote in list
        ]
        list_df = pd.DataFrame(data)
        return list_df


if __name__ == "__main__":
    latest_date = '2025-03-24'
    # print(APIUtils.get_trade_dates(latest_date))

    print(APIUtils.get_trade_dates_range(latest_date, -5))
    # 今天
    print(APIUtils.get_trade_dates_range(datetime.today().strftime('%Y-%m-%d'), -5))
    print(APIUtils.get_trade_dates_range(datetime.today().strftime('%Y-%m-%d'), 5))

    # ths_limit_up_dir = 'D:/stock/ths_data'
    # ths_xls = APIUtils.get_xls_files(ths_limit_up_dir)
    # print(ths_xls)
    #
    # #文件命名格式：limit_up_analysis_2024-11-04.xls，请提取日期
    # for file in ths_xls:
    #     # 正确提取日期部分
    #     date = file.split('_')[-1].split('.')[0]
    #     print(date)
    #
    #     # 从目录中找到日期为date的xls文件，文件名为：limit_up_analysis_{date}.xls
    #     xls_file = f'{ths_limit_up_dir}/limit_up_analysis_{date}.xls'
    #     # 读取xls文件，注意文件不存在时输出warn提示
    #     if not os.path.exists(xls_file):
    #         print(f"日期 {date} 的涨停板数据文件不存在，跳过")
    #         continue
    #     # 读取xls文件
    #     # df = pd.read_excel(xls_file, engine='xlrd')
    #     df = pd.read_csv(xls_file, sep='\t', encoding='gbk')
    #
    #     # 完整输出df前5行
    #     print(df.head(5))
    #     print(df.count())
    #     for index, row in df.iterrows():
    #         # 填充对象
    #         limit_up_board_quotes_ths = LimitUpBoardQuotesThs(
    #             date=date,
    #             symbol=row['代码'],
    #             name=row['    名称'],
    #             reason=row['涨停分析']
    #         )
    #         print(limit_up_board_quotes_ths)


    @staticmethod
    def read_excel_file(file_path):
        """
        读取文件，支持 .xlsx、.xls 和类似 CSV 格式。
        :param file_path: 文件路径
        :return: DataFrame
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件 {file_path} 不存在。")
        file_extension = os.path.splitext(file_path)[1].lower()
        if file_extension == '.xlsx':
            try:
                return pd.read_excel(file_path, engine='openpyxl')
            except Exception as e:
                print(f"读取 .xlsx 文件时出错: {e}")
        elif file_extension == '.xls':
            try:
                return pd.read_excel(file_path, engine='xlrd')
            except Exception as e:
                print(f"尝试以 Excel 格式读取 .xls 文件时出错: {e}")
                # 尝试以 CSV 格式读取
                encodings = ['utf-8', 'gbk', 'gb2312']
                for encoding in encodings:
                    try:
                        return pd.read_csv(file_path, sep='\t', encoding=encoding)
                    except UnicodeDecodeError:
                        print(f"以 {encoding} 编码读取文件失败，尝试下一个编码。")
                    except Exception as csv_e:
                        print(f"以 {encoding} 编码读取文件时出错: {csv_e}")
        elif file_extension in ['.csv', '.txt']:
            encodings = ['utf-8', 'gbk', 'gb2312']
            for encoding in encodings:
                try:
                    return pd.read_csv(file_path, sep='\t', encoding=encoding)
                except UnicodeDecodeError:
                    print(f"以 {encoding} 编码读取文件失败，尝试下一个编码。")
                except Exception as e:
                    print(f"以 {encoding} 编码读取文件时出错: {e}")
        else:
            raise ValueError("不支持的文件格式，请使用 .xlsx、.xls、.csv 或 .txt 格式。")
