from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class IfOp(Op):
    """
    Function:
        if条件表达式算子，用于判断条件表达式是否为True

    Attributes:
        table_index (int): 输入表格索引，默认为0。
        condition (str): 条件表达式。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 输入表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/flow_control_op/if_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.flow_control_op.if_op import *

# 创建一个包含condition和数据的模拟数据表
input_table = DataTable(
    name = "TestTable",
    data = {
        'index':[1,2,3,4],
        'value':[10,20,30,40]
    }
)
#查看输入内容
display(input_table)

# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(input_table)

# 实例化 IfOp 并运行，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
#属性列中可以通过table_index设置具体需要处理的输入表列表的某一个表，不设置即为0，condition为条件
if_op = IfOp(
    name='IfOp',
    attrs={
    'table_index':0,
    'condition':'value>40'
})
success = if_op.process(op_context)

#在算子设计中，只要输入数据表有满足condition的行，就算满足条件
if(success):
    print('输入表存在满足条件的数据！')
else:
    print('输入表不存在满足条件的数据！')
    """

    def compute(self, op_context: OpContext) -> bool:
        table_index = self.attrs.get("table_index", 0)
        target_table: DataTable = op_context.input_tables[table_index]
        condition = self.attrs.get("condition")

        row_num = target_table.query(condition).shape[0]
        # 表达式结果为空时，If算子失败
        if row_num == 0:
            op_context.output_tables.extend(op_context.input_tables)
            return False

        op_context.output_tables.extend(op_context.input_tables)
        return True


op_register.register_op(IfOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="table_index", type="int" ,desc="输入表格索引") \
    .add_attr(name="condition", type="str", desc="条件表达式")