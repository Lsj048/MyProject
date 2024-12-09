import json

import jsonpath

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromDictOp(Op):
    """
    Function:
        从字典列中提取列算子，支持 jsonpath 匹配方式，可参考：https://blog.csdn.net/qq_15994257/article/details/120047941

    Attributes:
        dict_column (str): 字典列名。
        is_json_dump (bool, optional): 是否对字典列进行 json 反序列化，默认为 False。
        extracted_column (str): 要提取的列名，支持 jsonpath 匹配方式。
        alias_column_name (str, optional): 提取后的列名，默认为提取的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_dict_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.extract_column_from_dict_op import *

# 创建一个输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        'need_extract_from_dict_column': [
            '{"name": "John", "age": 30}',
            '{"name": "Jane", "age": 25}'
        ]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子，提取的属性可以是一个或是多个，注意如果设置了is_json_dump属性，要注意extract_column的格式
extract_op = ExtractColumnFromDictOp(
    name="ExtractColumnFromDictOp",
    attrs={
        "dict_column": "need_extract_from_dict_column",
        "is_json_dump": True,
        "extracted_column": ["$.name","$.age"],
        "alias_column_name": ["extracted_name","extracted_age"],
    }
)

# 执行算子
success = extract_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        dict_column = self.attrs.get("dict_column")
        is_json_dump = self.attrs.get("is_json_dump", False)
        extracted_column = self.attrs.get("extracted_column")
        alias_column_name = self.attrs.get("alias_column_name")

        if isinstance(extracted_column, str):
            extracted_column = [extracted_column]
        if alias_column_name is None:
            alias_column_name = [col.split('.')[-1] for col in extracted_column]
        elif isinstance(alias_column_name, str):
            alias_column_name = [alias_column_name]

        if dict_column not in in_table.columns or len(extracted_column) != len(alias_column_name):
            return False

        for col in alias_column_name:
            in_table[col] = None

        for index, row in in_table.iterrows():
            dict_value = row.get(dict_column)
            if not dict_value:
                continue
            if is_json_dump:
                dict_value = json.loads(dict_value)
            for idx, col in enumerate(extracted_column):
                if not col:
                    continue
                extracted_values = jsonpath.jsonpath(dict_value, col)
                if extracted_values and len(extracted_values) > 0:
                    in_table.at[index, alias_column_name[idx]] = extracted_values[0]

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromDictOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="dict_column", type="str", desc="字典列名") \
    .add_attr(name="is_json_dump", type="bool", desc="是否对字典列进行 json 反序列化") \
    .add_attr(name="extracted_column", type="str", desc="提取的列名") \
    .add_attr(name="alias_column_name", type="str", desc="新列别名")
