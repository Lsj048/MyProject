import jsonpath

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromListDictOp(Op):
    """
    Function:
        从字典列表列中提取列算子，输出为特定值组成的列表

    Attributes:
        list_dict_column (str): 字典列表列名。
        extracted_column (str): 要提取的列名，支持 jsonpath 匹配方式。
        alias_column_name (str, optional): 提取后的列名，默认为提取的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_list_dict_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.extract_column_from_list_dict_op import *

# 创建一个输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        'need_extract_from_list_dict_column':[
            [{"name":"Tom","age":"15"},{"name":"Jane","age":"19"}],
            [{"name":"Li","age":"23"}]
        ]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子，提取的属性可以是一个或是多个
extract_op = ExtractColumnFromListDictOp(
    name="ExtractColumnFromListDictOp",
    attrs={
        "list_dict_column": "need_extract_from_list_dict_column",
        "extracted_column": "$.name",
        "alias_column_name": "extract_value"
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
        list_dict_column = self.attrs.get("list_dict_column")
        extracted_column = self.attrs.get("extracted_column")
        alias_column_name = self.attrs.get("alias_column_name", extracted_column.split('.')[-1])

        if list_dict_column not in in_table.columns:
            return False

        in_table[alias_column_name] = None
        for index, row in in_table.iterrows():
            list_dict_value = row.get(list_dict_column)
            if not list_dict_value:
                continue
            value_list = []
            for dict_value in list_dict_value:
                extracted_values = jsonpath.jsonpath(dict_value, extracted_column)
                if extracted_values and len(extracted_values) > 0:
                    value_list.append(extracted_values[0])
                else:
                    value_list.append(None)

            in_table.at[index, alias_column_name] = value_list

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromListDictOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="list_dict_column", type="str", desc="字典列表列名") \
    .add_attr(name="extracted_column", type="str", desc="提取的列名") \
    .add_attr(name="alias_column_name", type="str", desc="新列别名")
