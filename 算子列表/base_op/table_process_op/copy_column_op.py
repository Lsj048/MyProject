from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class CopyColumnOp(Op):
    """
    Function:
        两表之间列拷贝算子，拷贝单列到目标表，如果两表行数一致，则可以一一对应的拷贝，否则只能将源表第一行拷贝到目标表的所有行

    Attributes:
        source_column (str): 源列名
        target_column (str): 目标列名
        only_copy_first_row (bool): 是否仅拷贝第一行到目标表，默认为False

    InputTables:
        source_table: 输入表格
        target_table: 目标表格

    OutputTables:
        target_table: 拷贝后的目标表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/copy_column_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.copy_column_op import *

#设置源表和目标表
source_table = DataTable(
    name="SourceTable",
    data={
        's_column1':[123,23,21]
    }
)
target_table = DataTable(
    name="TargetTable",
    data={
        't_column1':[1,3]
    }
)
display(source_table,target_table)

# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(source_table)
op_context.input_tables.append(target_table)

# 实例化 CopyColumnOp 并运行,source_column为想要拷贝源表中的列名；target_column为想要拷贝到目的表的列名；only_copy_first_row标识是否只拷贝第一行
# 具体的想要查看各种情况（如源列名不存在；目的列名不存在；行数不相同等情况）请自行测试
copy_column_op = CopyColumnOp(
    name='CopyColumnOp',
    attrs={
        "source_column": "s_column1",
        "target_column": "t_column1",
        "only_copy_first_row": True
    }
)
success = copy_column_op.process(op_context)

# 检查是否启动，输出表为目标表
if success:
    output_table1 = op_context.output_tables[0]
    print("成功执行,拷贝后的目标表为：")
    display(output_table1)
else:
    print("执行失败！")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[1])
        return True

    def compute(self, op_context: OpContext) -> bool:
        source_table: DataTable = op_context.input_tables[0]
        target_table: DataTable = op_context.input_tables[1]
        source_column = self.attrs.get("source_column")
        target_column = self.attrs.get("target_column")
        only_copy_first_row = self.attrs.get("only_copy_first_row", False)

        if source_column not in source_table.columns:
            return False

        if only_copy_first_row:
            if source_table.shape[0] == 0:
                return False

            target_table[target_column] = [source_table[source_column][0]] * target_table.shape[0]
        else:
            if source_table.shape[0] != target_table.shape[0]:
                return False

            target_table[target_column] = source_table[source_column]
        op_context.output_tables.append(target_table)
        return True


op_register.register_op(CopyColumnOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="source_column", type="str" ,desc="源列名") \
    .add_attr(name="target_column", type="str", desc="目标列名") \
    .add_attr(name="only_copy_first_row", type="bool", desc="是否仅拷贝第一行到目标表")
