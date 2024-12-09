from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CumsumColumnOp(Op):
    """
    Function:
        计算某列的累积和算子，列值必须为数值类型（int64/float64）

    Attributes:
        source_column (str): 源列名
        target_column (str): 目标列名

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 增加累积和后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/cumsum_column_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.cumsum_column_op import *

#设置输入表，注意要计算列和的数据类型必须是int或者float
input_table = DataTable(
    name="TestTable",
    data={
        'need_get_sum_column':[123,23,21]
    }
)

# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(input_table)

# 实例化 CumsumColumnOp 并运行
cumsum_column_op = CumsumColumnOp(
    name='CumsumColumnOp',
    attrs={
        "source_column": "need_get_sum_column",
        "target_column": "sum_of_value",
    }
)
success = cumsum_column_op.process(op_context)

# 检查是否启动，输出表为目标表
if success:
    output_table = op_context.output_tables[0]
    print("成功执行！")
    display(output_table)
else:
    print("执行失败！")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        source_column = self.attrs.get("source_column")
        target_column = self.attrs.get("target_column")

        if source_column not in in_table.columns or in_table[source_column].dtype not in ['int64', 'float64']:
            return False

        in_table[target_column] = in_table[source_column].cumsum()
        op_context.output_tables.append(in_table)
        return True


op_register.register_op(CumsumColumnOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="source_column", type="str", desc="源列名") \
    .add_attr(name="target_column", type="str", desc="目标列名")