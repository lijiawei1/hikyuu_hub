from openpyxl.styles import GradientFill, Color
import pandas as pd
from datetime import datetime, timedelta

from openpyxl.styles.fills import Stop

# 生成日期序列
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 5)
dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# 示例股票数据（代码: 涨幅列表）
stocks = {
    '600000': [0.02, 0.015, -0.03, 0.05, 0.01],
    '000001': [0.01, -0.005, 0.02, 0.03, -0.02]
}

# 创建DataFrame并设置日期列
df = pd.DataFrame({'日期': dates})

# 添加股票数据列
col_index = 1
for code, changes in stocks.items():
    df.insert(col_index, f'股票{code}', code)
    df.insert(col_index + 1, f'涨幅{code}', changes)
    col_index += 2

# 写入Excel文件
with pd.ExcelWriter('stock_data.xlsx') as writer:
    df.to_excel(writer, index=False, sheet_name='Stock Data')

    # 获取工作表对象
    worksheet = writer.sheets['Stock Data']

    # 设置10级渐变色（红→黄→绿）
    gradient_fill = GradientFill(stop=[
        Stop(Color('FF0000'), position=0.0),
        Stop(Color('FF6666'), position=0.3),
        Stop(Color('FFFF00'), position=0.5),
        Stop(Color('99FF99'), position=0.7),
        Stop(Color('00FF00'), position=1.0)
    ])

    # 应用样式到所有涨幅列（奇数列）
    for col in range(2, df.shape[1] + 1, 2):
        for row in range(2, len(df) + 2):
            cell = worksheet.cell(row=row, column=col)

            # 根据涨跌幅值设置颜色强度
            value = float(cell.value)
            if value >= 0:
                ratio = min(value / 0.1, 1)  # 涨幅超过10%按最大值计算
            else:
                ratio = min(abs(value) / 0.05, 1)  # 跌幅超过5%按最大值计算

            # 动态调整颜色梯度
            cell.fill = GradientFill(stop=[
                Stop(Color('FF0000'), position=0.0),
                Stop(Color('FFFF00' if value >= 0 else 'FF6666'), position=ratio),
                Stop(Color('00FF00' if value >= 0 else '990000'), position=1.0)
            ])