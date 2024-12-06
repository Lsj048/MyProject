from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class EmptyTableCheck(Op):
    """
    Function:
        空表检查算子，空表则返回False

    Attributes:
        无

    InputTables:
        in_table: 输入表格

    OutputTables:
        无

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/empty_table_check.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.empty_table_check import *

        #设置输入表，注意要计算列和的数据类型必须是int或者float
        input_table = DataTable(
            name="TestTable",
            data={
                #'value':[12,1]
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 EmptyTableCheck 并运行
        empty_table_check = EmptyTableCheck(
            name='EmptyTableCheck',
            attrs={}
        )
        success = empty_table_check.process(op_context)

        # 检查是否启动，输出表为目标表
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("数据表为空！")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]

        if in_table.empty:
            self.fail_reason = f"表数据为空：{in_table.name}"
            self.trace_log.update({"fail_reason": self.fail_reason})
            return False

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(EmptyTableCheck) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder")
