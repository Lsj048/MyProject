from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AddTableRow(Op):
    """
    Function:
        在表里增加行，增加的行的列默认值为0/None

    Attributes:
        row_num (int): 增加的行数

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 增加行后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/add_table_row.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.add_table_row import *

        # 创建一个模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'value':[1.3,2.5,7.5]
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 AddTableRow 并运行,不指定row_num,则默认新增一行
        #可自行更改DataTable数据类型查看默认值变化情况
        add_table_row = AddTableRow(
            name='AddTableRow',
            attrs={
                "row_num":2
            }
        )
        success = add_table_row.process(op_context)

        # 检查是否启动
        if success:
            output_table = op_context.output_tables[0]
            print("成功执行！")
            display(output_table)
        else:
            print("执行失败！")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        row_num = self.attrs.get("row_num", 1)

        default_value = {}
        for col in in_table.columns:
            val = None
            if in_table[col].dtype in ["int64", "float64"]:
                val = 0
            default_value.update({col: val})

        for _ in range(row_num):
            in_table.loc[len(in_table)] = default_value

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(AddTableRow) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="row_num", type="int", desc="新增行数")
