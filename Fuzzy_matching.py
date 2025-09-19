import pandas as pd
import ahocorasick
import numpy as np
import os.path
import sys
import re

'''
源数据提取：从描述提取各关键字
'''
class Data_Extraction:
    def __init__(self):
        self.actual_execution_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen',False) else os.path.dirname(
            os.path.abspath(__file__))
        self.coding_data = pd.read_excel(r'Test_data.xlsx')
        # self.product_data = pd.read_excel(r'Product_data & Match_data.xlsx',sheet_name='Product')
        self.match_dic = pd.read_excel(r'Match_data_rule.xlsx',sheet_name='Brand')
        self.la_dic = pd.read_excel(r'Match_data_rule.xlsx',sheet_name='Label')
        self.k_brand = pd.read_excel(r'Match_data_rule.xlsx',sheet_name='Key_brand')

    def coding_data_solve(self):
        #批量生成字段
        self.coding_data = self.coding_data.assign(
            Pure_text_description = None,
            Link = None,
            BRAND = None,
            ITEM = None,
            Match_ID = None,
            Remark = None,
            Item_ID = None,
            System = None,
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

        '''
        描述拆解
        '''
        # 使用正则表达式提取链接到新列Link
        self.coding_data['Link'] = self.coding_data['Main Text'].apply(
            lambda x: re.findall(r'https?://[^\s]+', x)[0] if re.findall(r'https?://[^\s]+', x) else None)
        #纯文本提取
        self.coding_data["Pure_text_description"] = self.coding_data["Main Text"].str.split("|", expand=True)[3]

    def extraction_match(self):
        # ================= 品牌匹配 =================
        brand_map = dict(zip(self.match_dic["BRANDTEXT"], self.match_dic["EN_BR"]))

        brand_A = ahocorasick.Automaton()
        for brand, en_br in brand_map.items():
            brand_A.add_word(str(brand), (brand, en_br, len(str(brand))))
        brand_A.make_automaton()

        def extract_brand(desc):
            matches = [(length, en_br) for _, (brand, en_br, length) in brand_A.iter(str(desc))]
            if matches:
                return max(matches, key=lambda x: x[0])[1]  # 返回 EN_BR
            return None

        self.coding_data["BRAND"] = self.coding_data["Pure_text_description"].apply(extract_brand)

        # ================= 型号匹配 =================
        def normalize_text(s):
            if pd.isna(s) or s is None:
                return ""
            s = str(s).lower()
            return re.sub(r'[^0-9a-z\u4e00-\u9fff]+', '', s)

        model_dic = self.k_brand

        # 构建品牌 → automaton 字典
        model_automata = {}
        for brand in model_dic["BRANDTEXT"].dropna().unique():
            rows = model_dic[model_dic["BRANDTEXT"] == brand]
            models = [(str(row["MODELTEXT"]), normalize_text(row["MODELTEXT"])) for _, row in rows.iterrows()]
            models = [(orig, norm) for orig, norm in models if norm]
            # 按长度降序
            models.sort(key=lambda x: len(x[1]), reverse=True)

            # 去掉被包含的短词
            filtered = []
            seen = set()
            for orig, norm in models:
                if any(norm in longer for longer in seen):
                    continue
                filtered.append((orig, norm))
                seen.add(norm)

            # 构建 Automaton
            A = ahocorasick.Automaton()
            for orig, norm in filtered:
                A.add_word(norm, (orig, len(norm)))
            if filtered:
                A.make_automaton()
                model_automata[brand] = A

        # ================= 后续处理 =================
        def extract_item(row):
            brand = row['BRAND']
            desc = normalize_text(row['Pure_text_description'])
            if pd.isna(brand) or brand not in model_automata:
                return None
            matches = [(length, orig) for _, (orig, length) in model_automata[brand].iter(desc)]
            if matches:
                return max(matches, key=lambda x: x[0])[1]
            return None

        self.coding_data['ITEM'] = self.coding_data.apply(extract_item, axis=1)

        # 生成 Match_ID
        self.coding_data["Match_ID"] = self.coding_data.apply(
            lambda row: f"{row['BRAND']}-{row['ITEM']}"
            if pd.notna(row["BRAND"]) and pd.notna(row["ITEM"]) else None,
            axis=1
        )

        # 生成 Remark
        label_map = dict(zip(self.la_dic["KEY_BRAND"], self.la_dic["Label"]))

        def get_remark(brand):
            if brand in label_map and label_map[brand] == "Y":
                return "不可模糊匹配型号，请留意"
            return "按模糊匹配流程"

        self.coding_data["Remark"] = self.coding_data["BRAND"].apply(get_remark)

        # 映射 Item_ID
        id_map = dict(zip(self.k_brand["match_code"], self.k_brand["id"]))
        self.coding_data['Item_ID'] = self.coding_data['Match_ID'].map(id_map)

    def fuzzy_extraction(self):
        """
        从 Pure_text_description 中模糊匹配，填充 System、GAMING_CLAIM 等字段
        使用 ahocorasick 自动机实现最长匹配优先
        """
        import ahocorasick

        # sheet 与目标字段的对应关系
        sheet_field_map = {
            "System": "System",
            "Gaming": "GAMING_CLAIM",
            "Type": "HEADPHONE_TYPE",
            "C_or_H": "EARCUFF_or_EARHOOK",
            "Head": "HEADBAND",
            "Neck": "NECKBAND",
            "Act_Noise": "ACT_NOISE",
            "Bone": "BONE_CONDUCTION"
        }

        # 构造 {字段 → Automaton} 的字典
        automata_dict = {}
        for sheet, field in sheet_field_map.items():
            df = pd.read_excel(r"Match_data_rule.xlsx", sheet_name=sheet)
            dic = dict(zip(df.iloc[:, 0].dropna().astype(str), df.iloc[:, 1]))

            A = ahocorasick.Automaton()
            for k, v in dic.items():
                if pd.notna(k):
                    A.add_word(str(k), (k, v, len(str(k))))  # 保存关键词、值、长度
            if len(A) > 0:
                A.make_automaton()
                automata_dict[field] = A

        # 定义匹配函数（最长匹配优先）
        def match_field(text, A):
            if pd.isna(text) or not A:
                return None
            matches = [(length, val) for _, (key, val, length) in A.iter(str(text))]
            if matches:
                return max(matches, key=lambda x: x[0])[1]  # 返回匹配值
            return None

        # 遍历每个字段匹配
        for field, A in automata_dict.items():
            self.coding_data[field] = self.coding_data["Pure_text_description"].apply(
                lambda x: match_field(x, A)
            )

        # 默认值
        self.coding_data["GAMING_CLAIM"] = self.coding_data["GAMING_CLAIM"].fillna("NO GAMING CLAIM")
        self.coding_data["HEADBAND"] = self.coding_data["HEADBAND"].fillna("NO HEADBAND")
        # self.coding_data["ACT_NOISE"] = self.coding_data["ACT_NOISE"].fillna("NO ACTIVE NC")
        # self.coding_data["BONE_CONDUCTION"] = self.coding_data["BONE_CONDUCTION"].fillna("NO BONE CONDUCT")

        self.coding_data.to_excel(r'demo.xlsx', index=False)

        return self.coding_data

if __name__ == '__main__':
    c_d_s = Data_Extraction()
    c_d_s.coding_data_solve()
    c_d_s.extraction_match()
    c_d_s.fuzzy_extraction()