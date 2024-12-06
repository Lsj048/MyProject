from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableRowExtractOp(Op):
    """
    Function:
        表的行提取算子，建议用TableQueryOp

    Attributes:
        table_name (str): 表的名称
        column (str): 列名
        rule (str): 提取规则，可选值为 "in", "not_in", ">", "<", ">=", "<="
        value (Union[list, int, float]): 提取规则对应的值，类型根据规则而定

    InputTables:
        in_table: 输入表

    OutputTables:
        new_table: 提取后行组成的新表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_row_extract_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_row_extract_op import *

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
        table_row_extract_op = TableRowExtractOp(
            name="TableRowExtractOp",
            attrs={
                "table_name": "new_table",
                "column": "id",
                "rule":"<=",
                "value":1
            }
        )

        # 执行算子
        success = table_row_extract_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        table_name = self.attrs.get("table_name")
        column = self.attrs.get("column")
        rule = self.attrs.get("rule")
        value = self.attrs.get("value")

        rule_func_map = {
            "in": lambda x: x[column].isin(value),
            "not_in": lambda x: ~x[column].isin(value),
            ">": lambda x: x[column] > value,
            "<": lambda x: x[column] < value,
            ">=": lambda x: x[column] >= value,
            "<=": lambda x: x[column] <= value
        }

        rule_type_map = {
            "in": lambda x: isinstance(x, list),
            "not_in": lambda x: isinstance(x, list),
            ">": lambda x: isinstance(x, (int, float)),
            "<": lambda x: isinstance(x, (int, float)),
            ">=": lambda x: isinstance(x, (int, float)),
            "<=": lambda x: isinstance(x, (int, float))
        }

        if rule not in rule_type_map or not rule_type_map[rule](value):
            logger.info(f"params error, rule:{rule} value:{value},{type(value)} ")
            return False

        new_table = in_table[rule_func_map[rule](in_table)].copy()

        if new_table is not None:
            new_table.name = table_name
            op_context.output_tables.append(new_table)

        return True


op_register.register_op(TableRowExtractOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="table_name", type="str", desc="新表名") \
    .add_attr(name="column", type="str", desc="规则处理的列名") \
    .add_attr(name="rule", type="str", desc="规则类型，支持in、not_in、>、<、>=、<=") \
    .add_attr(name="value", type="str", desc="规则处理的值集合")
