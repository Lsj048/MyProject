from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class FlatDoubleListOp(Op):
    """
    【local】将双层list结构打平为一层

    Attributes:
        source_column (str): 源列名
        target_column (str): 目标列名

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入打平后列的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/flat_double_list_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.flat_double_list_op import *

        # 创建一个输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'need_flat_column':[
                    [[1,2],[3,4]],
                    [[5,6],[7,8]]
                ]
            }
        )
        display(input_table)
        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        flat_op = FlatDoubleListOp(
            name="FlatDoubleListOp",
            attrs={
                "source_column": "need_flat_column",
                "target_column": "flat_column",
            }
        )

        # 执行算子
        success = flat_op.process(op_context)

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
        source_column = self.attrs.get("source_column")
        target_column = self.attrs.get("target_column")

        in_table[target_column] = None
        for index, row in in_table.iterrows():
            flat_res = []
            first_list = row.get(source_column)
            for second_list in first_list:
                for item in second_list:
                    flat_res.append(item)

            in_table.at[index, target_column] = flat_res

        op_context.output_tables.append(in_table)
        return True

op_register.register_op(FlatDoubleListOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="source_column", type="str", desc="源列名") \
    .add_attr(name="target_column", type="str", desc="目的列名")