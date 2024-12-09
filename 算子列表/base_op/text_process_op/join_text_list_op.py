from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class JoinTextListOp(Op):
    """
    Function:
        文本list合并为一段文本

    Attributes:
        text_list_column (str): 待合并的文本list列名，默认为"text_list"。
        joined_text_column (str): 合并后的文本列名，默认为"joined_text"。
        join_symbol (str): 合并符号，默认为空字符串。

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/join_text_list_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.text_process_op.join_text_list_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "text_list": [["你好,我叫A。","你好你好，我叫B。"]]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
join_text_list_op = JoinTextListOp(
    name="JoinTextListOp",
    attrs={
        "text_list_column":"text_list",
        "joined_text_column":"merge_result",
        "join_symbol":""
    }
)

# 执行算子
success = join_text_list_op.process(op_context)

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
        text_list_column = self.attrs.get("text_list_column", "text_list")
        joined_text_column = self.attrs.get("joined_text_column", "joined_text")
        join_symbol = self.attrs.get("join_symbol", "")

        in_table[joined_text_column] = None
        for index, row in in_table.iterrows():
            text_list = row.get(text_list_column)
            joined_text = join_symbol.join(text_list)
            in_table.at[index, joined_text_column] = joined_text

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(JoinTextListOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="text_list_column", type="str", desc="待合并的文本list列名") \
    .add_attr(name="joined_text_column", type="str", desc="合并后的文本列名") \
    .add_attr(name="join_symbol", type="str", desc="合并符号")
