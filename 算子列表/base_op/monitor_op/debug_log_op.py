from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class DebugLogOp(Op):
    """
    Function:
        DebugLog调试算子，打印Table内容，用于调试和问题排查

    Attributes:
        print_columns (Union[str, List[str]]): 需要打印的列名，默认为None。
        logger_type (str): 日志级别，默认为"DEBUG"。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 输出表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/monitor_op/debug_log_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.monitor_op.debug_log_op import *

# 创建一个模拟数据表
input_table = DataTable(
    name = "TestTable",
    data = {
        'index':[1,2,3,4],
        'value':[1,23,12,43]
    }
)
# 设置 OpContext
op_context = OpContext("graph_name", "request_tag", "request_id")
op_context.input_tables.append(input_table)

# 实例化 DebugLogOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
#日志级别提供了三种，分别是info、debug、error，为设置则默认debug。print_column指定打印哪一列或哪几列，为设置则默认打印全部
debug_log_op = DebugLogOp(
    name='DebugLogOp',
    attrs={
    "print_columns": ["value","index"],
    #"logger_type": "INFO",
    #"logger_type": "DEBUG",
    "logger_type": "ERROR"
})
success = debug_log_op.process(op_context)
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        print_columns = self.attrs.get("print_columns", None)
        logger_type = self.attrs.get("logger_type", "DEBUG")
        if isinstance(print_columns, str):
            print_columns = [print_columns]

        logger_type_map = {
            "DEBUG": logger.debug,
            "INFO": logger.info,
            "ERROR": logger.error
        }

        if print_columns and isinstance(print_columns, list):
            print_columns_exist = [col for col in print_columns if col in in_table.columns]
            logger_type_map.get(logger_type)(f"{'=' * 20}{op_context.graph_name}-{op_context.request_id}{'=' * 20}\n"
                                             f"table_name: {in_table.name}\n"
                                             f"content:\n{in_table[print_columns_exist]}")
        else:
            logger_type_map.get(logger_type)(f"{'=' * 20}{op_context.graph_name}-{op_context.request_id}{'=' * 20}\n"
                                             f"table_name: {in_table.name}\n"
                                             f"content:\n{in_table}")

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(DebugLogOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="print_columns", type="list/str", desc="要打印的列") \
    .add_attr(name="logger_type", type="str", desc="日志打印类型")
