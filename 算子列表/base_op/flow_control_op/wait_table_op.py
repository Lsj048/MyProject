from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class WaitTableOp(Op):
    """
    Function:
        等待表全部ready算子，虽然与ElseOp、EndIfOp代码相同，但行为不一致，ElseOp、EndIfOp会在框架层面有对应的特殊逻辑，而WaitTableOp没有

    Attributes:
        无

    InputTables:
        多表输入

    OutputTables:
        多表输出

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/flow_control_op/wait_table_op.py?ref_type=heads

    Examples:
        #无需示例代码
    """
    def compute(self, op_context: OpContext) -> bool:
        op_context.output_tables.extend(op_context.input_tables)
        return True


op_register.register_op(WaitTableOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_input(name="...", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="...", type="DataTable", desc="placeholder")