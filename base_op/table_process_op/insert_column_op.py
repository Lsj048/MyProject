from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext
from video_graph.ops.light_function.function_manager import FunctionManager


class InsertColumnOp(Op):
    """
    Function:
        插入新列算子，支持以自定义函数方式计算新列值

    Attributes:
        column_name (str): 新列名
        function_name (str): 计算新列值的函数名

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/insert_column_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.insert_column_op import *

        # 创建一个输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'nums':[[1,2],[3,4]],
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        insert_column_op = InsertColumnOp(
            name="InsertColumnOp",
            attrs={
                "column_name": "new_column",
                "function_name": "list_merge",
            }
        )

        # 执行算子
        success = insert_column_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        column_name = self.attrs.get("column_name")
        function_name = self.attrs.get("function_name")

        function = FunctionManager().get_function(function_name)
        if function:
            in_table[column_name] = in_table.apply(function, axis=1)
        else:
            logger.error(f"insert column[{column_name}] failed, function_name[{function_name}] not found")

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(InsertColumnOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="column_name", type="str", desc="插入的列名") \
    .add_attr(name="function_name", type="str", desc="调用的函数名")
