import json

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class JsonDumpOp(Op):
    """
    Function:
        python对象转json列算子

    Attributes:
        source_column (str): 源列名
        target_column (str): 目标列名

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/json_dump_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.json_dump_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "source_content": [{"张三":"13岁"},{"李四":"14岁"}]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        join_dump_op = JsonDumpOp(
            name="JsonDumpOp",
            attrs={
                "source_column":"source_content",
                "target_column":"target_content",
            }
        )

        # 执行算子
        success = join_dump_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        source_column = self.attrs.get("source_column")
        target_column = self.attrs.get("target_column")

        in_table[target_column] = None
        for index, row in in_table.iterrows():
            object_col = row.get(source_column)
            in_table.at[index, target_column] = json.dumps(object_col)

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(JsonDumpOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="source_column", type="str", desc="源列名") \
    .add_attr(name="target_column", type="str", desc="目的列名")
