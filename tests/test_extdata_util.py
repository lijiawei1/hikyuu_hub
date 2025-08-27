import unittest
import tempfile
import pandas as pd
import os
from tdx.extdata_util import *


class TestTdxDataUpdate(unittest.TestCase):

    def setUp(self):
        """设置测试数据"""
        # 创建测试idx DataFrame
        self.test_idx_data = pd.DataFrame({
            'market_code': [0, 0, 0],
            'stock_code': ['000001', '000002', '000003'],
            'record_count': [3, 2, 4],
            'cum_sum': [0, 3, 5]
        })

        # 创建测试dat数据
        self.test_dat_data = {
            '000001': pd.DataFrame({
                'date_int': [20230101, 20230102, 20230103],
                'time_int': [0, 0, 0],
                'value_f': [1.0, 2.0, 3.0]
            }),
            '000002': pd.DataFrame({
                'date_int': [20230101, 20230102],
                'time_int': [0, 0],
                'value_f': [4.0, 5.0]
            }),
            '000003': pd.DataFrame({
                'date_int': [20230101, 20230102, 20230103, 20230104],
                'time_int': [0, 0, 0, 0],
                'value_f': [6.0, 7.0, 8.0, 9.0]
            })
        }

    def test_merge_stock_data(self):
        """测试合并股票数据"""
        # 创建增量数据
        new_data = pd.DataFrame({
            'date_int': [20230104, 20230105],
            'time_int': [0, 0],
            'value_f': [10.0, 11.0]
        })

        # 合并数据
        merged = merge_stock_data(self.test_dat_data['000001'], new_data)

        # 验证结果
        self.assertEqual(len(merged), 5)  # 3条旧数据 + 2条新数据
        self.assertEqual(merged['date_int'].iloc[-1], 20230105)  # 最后一条是新数据
        self.assertEqual(merged['value_f'].iloc[-1], 11.0)

    def test_merge_stock_data_with_duplicates(self):
        """测试合并有重复日期的股票数据"""
        # 创建有重复日期的增量数据
        new_data = pd.DataFrame({
            'date_int': [20230102, 20230104],  # 20230102是重复日期
            'time_int': [0, 0],
            'value_f': [20.0, 21.0]  # 新值
        })

        # 合并数据
        merged = merge_stock_data(self.test_dat_data['000001'], new_data)

        # 验证结果
        self.assertEqual(len(merged), 4)  # 去重后应该有4条记录
        # 验证重复日期的数据使用了新值
        feb2_data = merged[merged['date_int'] == 20230102]
        self.assertEqual(feb2_data['value_f'].iloc[0], 20.0)

    def test_merge_stock_data_limit_500(self):
        """测试合并数据时限制500条记录"""
        # 创建大量测试数据
        many_dates = list(range(20230001, 20230501))  # 500个日期
        many_data = pd.DataFrame({
            'date_int': many_dates,
            'time_int': [0] * 500,
            'value_f': range(500)
        })

        # 合并数据（应该被限制为500条）
        merged = merge_stock_data(self.test_dat_data['000001'], many_data)

        # 验证结果
        self.assertEqual(len(merged), 500)  # 应该被限制为500条
        self.assertEqual(merged['date_int'].min(), 20230001)  # 最早的数据
        self.assertEqual(merged['date_int'].max(), 20230499)  # 最新的数据

    def test_generate_idx_file(self):
        """测试生成idx文件"""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test.idx")

            # 生成idx文件
            success = generate_idx_file(self.test_idx_data, output_path)

            # 验证结果
            self.assertTrue(success)
            self.assertTrue(os.path.exists(output_path))
            self.assertGreater(os.path.getsize(output_path), 0)

    def test_process_incremental_update(self):
        """测试处理增量更新"""
        # 创建增量数据
        new_idx_data = pd.DataFrame({
            'market_code': [0, 0, 0],
            'stock_code': ['000001', '000002', '000003'],
            'record_count': [2, 1, 3],
            'cum_sum': [0, 2, 3]
        })

        new_dat_data = {
            '000001': pd.DataFrame({
                'date_int': [20230104, 20230105],
                'time_int': [0, 0],
                'value_f': [10.0, 11.0]
            }),
            '000002': pd.DataFrame({
                'date_int': [20230103],
                'time_int': [0],
                'value_f': [12.0]
            }),
            '000003': pd.DataFrame({
                'date_int': [20230105, 20230106, 20230107],
                'time_int': [0, 0, 0],
                'value_f': [13.0, 14.0, 15.0]
            })
        }

        # 处理增量更新
        updated_idx, updated_dat = process_incremental_update(
            self.test_idx_data, self.test_dat_data,
            new_idx_data, new_dat_data
        )

        # 验证结果
        self.assertFalse(updated_idx.empty)
        self.assertEqual(len(updated_dat), 3)

        # 验证股票000001的数据
        stock_000001 = updated_dat['000001']
        self.assertEqual(len(stock_000001), 5)  # 3旧 + 2新
        self.assertEqual(stock_000001['date_int'].iloc[-1], 20230105)  # 最新日期

        # 验证idx记录
        self.assertEqual(updated_idx[updated_idx['stock_code'] == '000001']['record_count'].iloc[0], 5)


if __name__ == '__main__':
    unittest.main()