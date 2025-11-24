from hikyuu import *

import akshare as ak
df = ak.bond_zh_us_rate("20221219")
x = df_to_ind(df, '美国国债收益率10年', '日期')
print(df.dtypes)
print(df.head())

print(x)
