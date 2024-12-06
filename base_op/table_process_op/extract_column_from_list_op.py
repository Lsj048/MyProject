import random

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExtractColumnFromListOp(Op):
    """
    Function:
        从列表列中提取列算子，支持提取指定索引的值到新列

    Attributes:
        list_column (str): 列表列名。
        extracted_index (int): 要提取的索引，设为-1则随机选取。
        alias_column_name (str, optional): 提取后的列名，默认为提取的列名。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 加入新列后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/extract_column_from_list_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.extract_column_from_list_op import *

        # 创建一个输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'need_extract_from_list_column': [
                    [1,2,3],
                    [4,5,6],
                    [7,8,9]
                ]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        extract_op = ExtractColumnFromListOp(
            name="ExtractColumnFromListOp",
            attrs={
                "list_column": "need_extract_from_list_column",
                "extracted_index": 0,
                "alias_column_name": "extracted_value",
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
        list_column = self.attrs.get("list_column")
        extracted_index = self.attrs.get("extracted_index")
        alias_column_name = self.attrs.get("alias_column_name", f"{list_column}_{extracted_index}")

        if list_column in in_table.columns:
            in_table[alias_column_name] = in_table[list_column].apply(
                lambda x: x[extracted_index] if x and 0 <= extracted_index < len(x) else x[
                    random.choice(range(len(x)))])

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(ExtractColumnFromListOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="list_column", type="str", desc="列表列名") \
    .add_attr(name="extracted_index", type="int", desc="提取的索引") \
    .add_attr(name="alias_column_name", type="str", desc="新列别名")
