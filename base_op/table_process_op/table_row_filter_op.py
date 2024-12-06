from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableRowFilterOp(Op):
    """
    Function:
        表的行过滤算子，建议用TableQueryOp

    Attributes:
        inplace (bool): 是否在原表上进行操作，默认为 True
        table_name (str): 表的名称
        reverse (bool): 是否反向过滤，默认为 False
        column (str): 列名
        rule (str): 提取规则，可选值为 "=", "in", ">", "<", "contains", "is_empty"
        value (Union[list, int, float, str]): 提取规则对应的值，类型根据规则而定

    InputTables:
        in_table: 输入表

    OutputTables:
        过滤后的新表，如果 inplace 为 True，则输出表为原表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_row_filter_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_row_filter_op import *

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
        #reverse 为true则过滤掉满足条件的行，false则保留满足条件的行
        #rule为提取规则，可选值为 "=", "in", ">", "<", "contains", "is_empty"
        #value提取规则对应的值，类型根据规则而定
        table_row_filter_op = TableRowFilterOp(
            name="TableRowFilterOp",
            attrs={
                "inplace": False,
                "table_name": "filtered_table",
                "column": "nums",
                "reserve": False,
                "rule": "in",
                "value": [23]
            }
        )

        # 执行算子
        success = table_row_filter_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        inplace = self.attrs.get("inplace", True)
        table_name = self.attrs.get("table_name")
        reverse = self.attrs.get("reverse", False)
        column = self.attrs.get("column")
        rule = self.attrs.get("rule")
        value = self.attrs.get("value")

        rule_func_map = {
            "=": lambda x: ~x[column].isin([value]) if reverse else x[column].isin([value]),
            "in": lambda x: ~x[column].isin(value) if reverse else x[column].isin(value),
            ">": lambda x: x[column] <= value if reverse else x[column] > value,
            "<": lambda x: x[column] >= value if reverse else x[column] < value,
            "contains": lambda x: x[column].apply(lambda l: value not in l if l else True) if reverse else x[
                column].apply(lambda l: value in l if l else False),
            "is_empty": lambda x: x[column].apply(lambda l: True if l else False) if reverse else x[column].apply(
                lambda l: False if l else True)
        }

        rule_type_map = {
            "=": lambda x: isinstance(x, (int, float, str, bool)),
            "in": lambda x: isinstance(x, list),
            ">": lambda x: isinstance(x, (int, float)),
            "<": lambda x: isinstance(x, (int, float)),
            "contains": lambda x: isinstance(x, (int, float, str, bool)),
            "is_empty": lambda x: True
        }

        if rule not in rule_func_map or not rule_type_map[rule](value) or \
                column not in in_table.columns:
            logger.info(f"params error, column:{column}, rule:{rule} value:{value},{type(value)} ")
            return False

        condition = rule_func_map[rule](in_table)
        new_df = in_table.drop(in_table[condition].index, inplace=inplace)

        if not inplace:
            new_table = DataTable(name=table_name, data=new_df)
            op_context.output_tables.append(new_table)
        else:
            op_context.output_tables.append(in_table)
        return True


op_register.register_op(TableRowFilterOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="inplace", type="bool", desc="是否就地处理") \
    .add_attr(name="table_name", type="str", desc="新表名") \
    .add_attr(name="reverse", type="bool", desc="是否反向过滤") \
    .add_attr(name="column", type="str", desc="规则处理的列名") \
    .add_attr(name="rule", type="str", desc="规则类型，支持=、in、>、<、contains、is_empty") \
    .add_attr(name="value", type="str", desc="规则处理的值集合")
