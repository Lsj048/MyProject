from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class BuildTableOp(Op):
    """
    Function:
        构建空表算子，构建特定表名和列名的空表，为了构建成功，至少包含一个默认列"not_defined"

    Attributes:
        table_name (str): 表名
        column_list (list[str]): 列名列表，默认为["not_defined"]

    InputTables:
        无

    OutputTables:
        new_table: 一张空表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/build_table_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.build_table_op import *


# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(input_table)

# 实例化 BuildTableOp 并运行
build_table_op = BuildTableOp(
    name='BuildTableOp',
    attrs={
        "table_name": "测试表",
        "column_list": ["第一列","第二列"]
    }
)
success = build_table_op.process(op_context)

# 检查是否启动
if success:
    output_table = op_context.output_tables[0]
    print("成功执行！")
    display(output_table)
else:
    print("执行失败！")

#可以结合AddTableRow为新建的表增加默认数据
    """

    def compute(self, op_context: OpContext) -> bool:
        table_name = self.attrs.get("table_name")
        column_list = self.attrs.get("column_list", ["not_defined"])
        new_table = DataTable(name=table_name, columns=column_list)
        op_context.output_tables.append(new_table)
        return True


op_register.register_op(BuildTableOp) \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="table_name", type="str", desc="表名") \
    .add_attr(name="column_list", type="list", desc="列名list")
