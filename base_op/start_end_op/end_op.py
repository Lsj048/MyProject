from video_graph.common.utils.logger import logger
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class EndOp(Op):
    """
    Function:
        返回特定表的停止算子

    Attributes:
        target_table_name (str): 目标表名。

    InputTables:
        多表输入，不限制输入表的数量。
    OutputTables:
        目标表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/start_end_op/end_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.start_end_op.end_op import *

        # 创建一个终止表的模拟数据表，表明需要与算子属性的target_table_name相同！
        input_table = DataTable(
            name = "EndTable",
            data = {
                'value':[123]
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 EndOp 并运行
        end_op = EndOp(
            name='EndOp',
            attrs={
                "target_table_name":'EndTable'
            }
        )
        success = end_op.process(op_context)

        # 检查是否启动
        if success:
            output_table = op_context.output_tables[0]
            print("成功结束！")
            display(output_table)
        else:
            print("算子结束失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        target_table_name = self.attrs.get("target_table_name")
        for table in op_context.input_tables:
            if table.name == target_table_name:
                op_context.output_tables.append(table)
                break

        if len(op_context.output_tables) == 0:
            logger.info(f"input table error, target_table_name[{target_table_name}] lack")
            return False

        return True


op_register.register_op(EndOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_input(name="...", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="target_table_name", type="str", desc="目标表名")
