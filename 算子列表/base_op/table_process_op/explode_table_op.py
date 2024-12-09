from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ExplodeTableOp(Op):
    """
    Function:
        按列炸开算子，对输入列表做展开，可以选择只保留部分列，默认保留全部列

    Attributes:
        table_name (str): 炸开后的表格名称。
        explode_column (str): 要炸开的列名。
        reserve_column_list (list, optional): 保留的列名列表，默认为None。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        new_table: 炸开后的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/explode_table_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.explode_table_op import *

#设置输入表
input_table = DataTable(
    name="TestTable",
    data={
        'need_explode_column':[[1,2,3]],
        'column1':12
    }
)
display(input_table)

# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(input_table)

# 实例化 ExplodeTableOp 并运行
#table_name为炸开后新表的名称，explode_column为需要炸开的原表列名，reserve_column_list为原表中想要保留的列，默认为全部保留，
explode_column_op = ExplodeTableOp(
    name='ExplodeTableOp',
    attrs={
        "table_name": "new_table",
        "explode_column": "need_explode_column",
        #"reserve_column_list": ""
    }
)
success = explode_column_op.process(op_context)

# 检查是否启动，输出表为目标表
if success:
    output_table = op_context.output_tables[0]
    print("成功执行！")
    display(output_table)
else:
    print("执行失败！")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        table_name = self.attrs.get("table_name")
        explode_column = self.attrs.get("explode_column")
        reserve_column_list = self.attrs.get("reserve_column_list")

        new_df = in_table.explode(explode_column).reset_index(drop=True)
        if reserve_column_list:
            new_df = new_df[reserve_column_list]
        new_table = DataTable(name=table_name, data=new_df)
        op_context.output_tables.append(new_table)
        return True


op_register.register_op(ExplodeTableOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="table_name", type="str", desc="新表名") \
    .add_attr(name="explode_column", type="str", desc="炸开的列名") \
    .add_attr(name="reserve_column_list", type="list", desc="保留的列名列表")
