from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableShuffleOp(Op):
    """
    Function:
        表随机打散算子，按行随机

    Attributes:
        无

    InputTables:
        in_table: 输入表格

    OutputTables:
        new_table: 打散后的新表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_shuffle_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_shuffle_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'id':[1,2,3],
                'nums':[12,23,35]
            }
        )
        display(input_table)

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        table_shuffle_op = TableShuffleOp(
            name="TableShuffleOp",
            attrs={}
        )

        # 执行算子
        success = table_shuffle_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]

        new_df = in_table.sample(frac=1)
        new_table = DataTable(name=in_table.name, data=new_df)

        op_context.output_tables.append(new_table)
        return True


op_register.register_op(TableShuffleOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder")
