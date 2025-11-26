# 使用绝对导入
import shutil

from hikyuu.interactive import *

from tdx import extdata_util
from tdx.ma50.preprocessor import Preprocessor
from tdx.workflow import Config, WorkflowLogger, BaseProcessor, Workflow


class TempDirProcessor(BaseProcessor):
    def process(self, data: any) -> (any, bool):
        # 临时目录
        temp_extdata_path = self.get_temp_extdata_path()
        self.logger.info(f"临时数据目录：{temp_extdata_path}")

        # 使用当前日期在临时目录下新建文件夹，作为后面使用存放临时数据，如果目录存在，则清空
        temp_dir_name = str(data['today_int'])
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

            data['temp_dir_path'] = temp_dir_path
            return data, True
        except Exception as e:
            self.logger.error(f"清理临时目录失败：{e}")
            return data, False

class StockListProcessor(BaseProcessor):

    def process(self, data: any) -> (any, bool):
        # 保存数据的路径
        temp_dir_path = data['temp_dir_path']

        def stock_filter(stk: Stock):
            # 上市一年以上
            # ipc_1year = (stk.start_datetime + Days(365) < Datetime.today())
            # start_0 = stk.code.startswith('000')
            not_st = 'ST' not in stk.name
            # 所属市场如下
            return (
                # ipc_1year and
                # start_0 and
                # not_st and
                (stk.type == constant.STOCKTYPE_A or
                 stk.type == constant.STOCKTYPE_GEM or
                 stk.type == constant.STOCKTYPE_START or
                 stk.type == constant.STOCKTYPE_A_BJ))

        stk_list = sm.get_stock_list(stock_filter)
        data['stock_list'] = stk_list

        return data, True


class CrowdednessProcessor(BaseProcessor):
    def process(self, data):

        temp_dir_path = data['temp_dir_path']
        stk_list = data['stock_list']

        # 调用hikyuu计算拥挤度
        percent = 5
        q = Query(-10)
        cal = sm.get_trading_calendar(q)
        df = pd.DataFrame()
        for dtime in cal:
            ind_view_df = get_inds_view(stk_list, [AMO], dtime)
            # print(ind_view_df.head())
            # 计算前5
            sorted_df = ind_view_df.sort_values('AMO', ascending=False).reset_index(drop=True)
            # 计算需要取的行数
            count = max(1, int(len(sorted_df) * percent / 100))
            # 取前N%的数据
            top_data = sorted_df.head(count)

            # 计算成交金额总和
            top_percent_sum = top_data['AMO'].sum()

            # print(str(count) + '  -  ' + str(dtime) + '  -  ' + str(top_percent_sum))
            # print(top_data.head(10))
            new_data = {
                'datetime': [dtime.datetime()],
                'AMO': [top_percent_sum]
            }

            new_df = pd.DataFrame(new_data)
            df = pd.concat([df, new_df], ignore_index=True)

        # print(df)
        amo_sum_ind = df_to_ind(df, col_name='AMO', col_date='datetime')
        # print(amo_sum_ind)

        s = sm['sh880001']
        k = s.get_kdata(q)
        amo_a_total_ind = AMO(k)

        amo_crowd_ind = amo_sum_ind / amo_a_total_ind
        # print(amo_crowd_ind)
        amo_crowd_ind.plot()

        cal_df = cal.to_df()
        amo_df = amo_sum_ind.to_df()

        # 关键步骤：按列合并（横向合并），使 datetime 和 AMO 成为两列
        # 前提：两个 DataFrame 的行数一样，且顺序一一对应
        amo_a_total_df = amo_a_total_ind.to_df().rename(columns={'value0': 'AMO_A'})

        result_df = pd.concat([amo_df, amo_a_total_df], axis=1).rename(columns={'value0': 'AMO'})
        # print(result_df)
        result_df['date_int'] = df['datetime'].dt.strftime('%Y%m%d').astype(int)
        result_df['value_f'] = result_df['AMO'] / result_df['AMO_A']
        # result_df = result_df.rename(columns={'AMO': 'value_f'})
        # print(result_df)
        result_df.to_csv(temp_dir_path + "\\extdata_82.csv")
        extdata_util.generate_file_dat(result_df, temp_dir_path + "\\extdata_82.dat")


        return data, True

class NLNHProcessor(BaseProcessor):
    def process(self, data: any) -> (any, bool):

        temp_dir_path = data['temp_dir_path']
        stk_list = data['stock_list']

        # 统计新高新低
        NH_60 = HIGH() >= HHV(HIGH(), 60)
        NL_60 = LOW() <= LLV(LOW(), 60)

        q = Query(-100, recover_type=Query.FORWARD)
        nh_60_ind = INSUM(stk_list, q, ind=NH_60, mode=0)
        nl_60_ind = INSUM(stk_list, q, ind=NL_60, mode=0)

        cal = sm.get_trading_calendar(q)
        cal_df = cal.to_df()

        nh_60_df = nh_60_ind.to_df().rename(columns={'value0': 'value_f'})
        nl_60_df = nl_60_ind.to_df().rename(columns={'value0': 'value_f'})

        result_df = pd.concat([cal_df, nh_60_df], axis=1).rename(columns={'value0': 'value_f'})
        result_df['date_int'] = result_df['datetime'].dt.strftime('%Y%m%d').astype(int)

        print(result_df)

        result_df.to_csv(temp_dir_path + "\\extdata_65.csv")
        extdata_util.generate_file_dat(result_df, temp_dir_path + "\\extdata_65.dat")


        result_df = pd.concat([cal_df, nl_60_df], axis=1).rename(columns={'value0': 'value_f'})
        result_df['date_int'] = result_df['datetime'].dt.strftime('%Y%m%d').astype(int)

        print(result_df)
        result_df.to_csv(temp_dir_path + "\\extdata_66.csv")
        extdata_util.generate_file_dat(result_df, temp_dir_path + "\\extdata_66.dat")

        return data, True




class MA50Processor(BaseProcessor):
    def process(self, data):

        # 保存数据的路径
        temp_dir_path = data['temp_dir_path']
        stk_list = data['stock_list']

        UP_MA50 = CLOSE() > MA(CLOSE(), 50)

        # 统计这批票里面上MA50的数量
        q = Query(-100, recover_type=Query.FORWARD)
        up_ma50_ind = INSUM(stk_list, q, ind=UP_MA50, mode=0)
        print(up_ma50_ind)

        cal = sm.get_trading_calendar(q)
        cal_df = cal.to_df()

        up_ma50_df = up_ma50_ind.to_df().rename(columns={'value0': 'value_f'})

        result_df = pd.concat([cal_df, up_ma50_df], axis=1).rename(columns={'value0': 'value_f'})
        result_df['date_int'] = result_df['datetime'].dt.strftime('%Y%m%d').astype(int)
        print(result_df)

        result_df.to_csv(temp_dir_path + "\\extdata_63.csv")
        extdata_util.generate_file_dat(result_df, temp_dir_path + "\\extdata_63.dat")

        return data, True


class EndProcessor(BaseProcessor):

    def process(self, data: any) -> (any, bool):
        self.logger.info(f"清理资源")
        data['stock_list'] = []
        return data, True


class CopyResourceProcessor(BaseProcessor):

    def process(self, data: any) -> (any, bool):
        self.logger.info(f"开始复制资源文件")

        try:
            # 从工作TDX目录复制新的info文件到临时目录
            info_file_path = self.get_info_file_path()
            temp_dir_path = data['temp_dir_path']

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
    config = Config("ind_config.yaml")

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
    workflow.add_processor(StockListProcessor(config, logger))
    # workflow.add_processor(CrowdednessProcessor(config, logger))
    # workflow.add_processor(CrowdednessProcessor(config, logger))
    # workflow.add_processor(MA50Processor(config, logger))
    workflow.add_processor(NLNHProcessor(config, logger))

    # workflow.add_processor(ApppendProcessor(config, logger))
    # workflow.add_processor(CopyResourceProcessor(config, logger))
    workflow.add_processor(EndProcessor(config, logger))

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