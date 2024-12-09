from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class ElseOp(Op):
    """
    Function:
        else分支算子，与IfOp配合使用，用于处理IfOp条件表达式为False的情况

    Attributes:
        无

    InputTables:
        多表输入

    OutputTables:
        多表输出

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/flow_control_op/else_op.py?ref_type=heads

    Examples:
        #无需示例代码～
    """

    def compute(self, op_context: OpContext) -> bool:
        op_context.output_tables.extend(op_context.input_tables)
        return True


op_register.register_op(ElseOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder")
