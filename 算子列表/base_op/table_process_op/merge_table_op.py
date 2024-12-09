import pandas

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class MergeTableOp(Op):
    """
    Function:
        多表合并算子，表格按行合并，输出表取输入表的列的并集，缺失列填充为 None

    Attributes:
        table_name (str): 合并后的表格名称。

    InputTables:
        多表输入，不限制输入表的数量。

    OutputTables:
        new_table: 合并后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/merge_table_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.merge_table_op import *

# 创建两个输入表格
input_table1 = DataTable(
    name="TestTable1",
    data = {
        'id1':[1,2,3],
        'nums1':[12,23,35]
    }
)
input_table2 = DataTable(
    name="TestTable2",
    data = {
        'id2':[1,2,3],
        'nums2':[4,5,6]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table1)
op_context.input_tables.append(input_table2)

# 配置并实例化算子,
#table_name：合并后新表的名称
merge_table_op = MergeTableOp(
    name="MergeTableOp",
    attrs={
        "table_name": "new_table",
    }
)

# 执行算子
success = merge_table_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        table_name = self.attrs.get("table_name")

        table_list = []
        for table in op_context.input_tables:
            table_list.append(table)

        new_df = pandas.concat(table_list)
        new_table = DataTable(name=table_name, data=new_df)
        op_context.output_tables.append(new_table)

        return True


op_register.register_op(MergeTableOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_input(name="...", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="table_name", type="str", desc="新表名")
