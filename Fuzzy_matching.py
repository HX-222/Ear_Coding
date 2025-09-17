import pandas as pd
import numpy as np
import os.path
import sys
import random


'''
源数据提取：从描述提取各关键字
'''
class Data_Extraction:
    def __init__(self):
        self.actual_execution_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen',False) else os.path.dirname(
            os.path.abspath(__file__))
        self.coding_data = pd.read_excel(r'Test_data.xlsx')
        #self.product_data = pd.read_excel(r'Product_data & Match_data.xlsx',sheet_name='Product')

    def coding_data_solve(self):
        #批量生成字段
        self.coding_data = self.coding_data.assign(
            Pure_text_description = None,
            Link = None,
            BRAND = None,
            ITEM = None,
            Match_ID = None,
            Item_ID = None,
            Whether_or_not = None,
            GAMING_CLAIM = None,
            HEADPHONE_TYPE = None,
            EARCUFF_or_EARHOOK = None,
            HEADBAND = None,
            NECKBAND = None,
            ACT_NOISE = None,
            BONE_CONDUCTION = None
        )

        # 源数据清洗
        self.coding_data['Price'] = (self.coding_data['Price'].astype(str).str.replace('.', '', regex=False).
                                     str.replace(',', '.', regex=False))
        self.coding_data["Price"] = pd.to_numeric(
            self.coding_data["Price"], errors="coerce").round().astype("Int64")

        self.coding_data.to_excel(r'demo.xlsx',index=False)

'''
数据匹配，精确匹配和模糊匹配
'''
class Data_Match:
    pass




if __name__ == '__main__':
    c_d_s = Data_Extraction()
    c_d_s.coding_data_solve()