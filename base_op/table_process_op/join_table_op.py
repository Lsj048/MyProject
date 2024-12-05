import pandas

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class JoinTableOp(Op):
    """
    【local】两表 join 算子，支持 left/right/inner/outer 四种 join 方式

    Attributes:
        table_name (str): join 后的表格名称
        on_column (str): join 的列
        how (str, optional): join 方式, left/right/inner/outer, 默认 inner

    InputTables:
        两张表, 第一张表为左表, 第二张表为右表

    OutputTables:
        new_table: join 后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/join_table_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.join_table_op import *

        # 创建两个输入表格
        input_table1 = DataTable(
            name="TestTable1",
            data = {
                'id':[1,2,3],
                'nums':[12,23,35]
            }
        )
        input_table2 = DataTable(
            name="TestTable2",
            data = {
                'id':[2,3,4],
                'nums':[4,5,6]
            }
        )
        display(input_table1,input_table2)
        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table1)
        op_context.input_tables.append(input_table2)

        # 配置并实例化算子,
        #how: join的方式, left/right/inner/outer, 默认 inner,可以都试试
        #如果两表on_column列的行数据相同则都保留同行的其他数据数据，如果不同，则根据how方式选择保留哪一方的数据
        join_table_op = JoinTableOp(
            name="JoinTableOp",
            attrs={
                "table_name": "new_table",
                "on_column": "id",
                "how":"left"
            }
        )

        # 执行算子
        success = join_table_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        table_name = self.attrs.get('table_name')
        on_column = self.attrs.get('on_column')
        how = self.attrs.get('how', 'inner')

        left_table = op_context.input_tables[0]
        right_table = op_context.input_tables[1]

        new_df = pandas.merge(left_table, right_table, on=on_column, how=how)
        new_table = DataTable(name=table_name, data=new_df)
        op_context.output_tables.append(new_table)

        return True


op_register.register_op(JoinTableOp) \
    .add_input(name="left_table", type="DataTable", desc="左表") \
    .add_input(name="right_table", type="DataTable", desc="右表") \
    .add_output(name="new_table", type="DataTable", desc="join 出来的表") \
    .add_attr(name="table_name", type="str", desc="join 后的表格名称") \
    .add_attr(name="extracted_column", type="str", desc="join 的列") \
    .add_attr(name="how", type="str", desc="join 方式, left/right/inner/outer")
