from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class SplitTextOp(Op):
    """
    Function:
        文本list合并为一段文本

    Attributes:
        text_column (str): 待合并的文本列名，默认为"text"。
        text_list_column (str): 合并后的文本列名，默认为"text_list"。
        split_symbol (str): 合并符号，默认为"。"。

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/split_text_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.split_text_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "text_list": ["你好我叫A。你好我叫B。"]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
split_text_op = SplitTextOp(
    name="SplitTextOp",
    attrs={
        "text_column":"text_list",
        "text_list_column":"merge_result",
        "split_symbol":"。"
    }
)

# 执行算子
success = split_text_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        text_column = self.attrs.get("text_column", "text")
        text_list_column = self.attrs.get("text_list_column", "text_list")
        split_symbol = self.attrs.get("split_symbol", "。")

        in_table[text_list_column] = None
        for index, row in in_table.iterrows():
            text = row.get(text_column)
            text_list = text.split(split_symbol)
            in_table.at[index, text_list_column] = text_list

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(SplitTextOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="text_column", type="str", desc="待合并的文本列名") \
    .add_attr(name="text_list_column", type="str", desc="合并后的文本列名") \
    .add_attr(name="split_symbol", type="str", desc="合并符号")
