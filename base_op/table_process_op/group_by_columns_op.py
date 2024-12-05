import json

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext
from video_graph.ops.light_function.function_manager import FunctionManager


class GroupByColumnsOp(Op):
    """
    【local】按列聚合算子，支持多列分组，多列聚合，聚合规则支持内置函数和自定义函数

    Attributes:
        group_by_columns (list): 分组列名列表。
        agg_rules (dict): 聚合规则，键为列名，值为聚合函数名或函数。
        table_name (str): 新表的名称。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        new_table: 聚合后的新表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/group_by_columns_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.group_by_columns_op import *

        # 创建一个输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'person':["李四","李四","张三","张三"],
                'dialog':[
                    ["我叫李四"],
                    ["是名工人"],
                    ["我叫张三"],
                    ["是名律师"],
                ]
            }
        )
        display(input_table)
        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        #1. group_by_columns，分组参考列名列表，会对列中的每个数据计算hash值，hash值相同的行分为一组
        #2. agg_rules，键存放需要分组处理的列名，值存放处理函数名称。分组规则按1中分好的行数进行分组
        group_column_op = GroupByColumnsOp(
            name="GroupByColumnsOp",
            attrs={
                "group_by_columns": ['person'],
                "agg_rules": json.dumps({"dialog":"list_merge"}),
                "table_name": "new_table"
            }
        )

        # 执行算子
        success = group_column_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        group_by_columns = self.attrs.get("group_by_columns", [])
        agg_rules = json.loads(self.attrs.get("agg_rules", "{}"))
        table_name = self.attrs.get("table_name", f"new_{in_table.name}")

        new_agg_rules = {}
        function_manager = FunctionManager()
        for column, function_name in agg_rules.items():
            function = function_manager.get_function(function_name)
            if function:
                new_agg_rules.update({column: function})
            else:
                new_agg_rules.update({column: function_name})

        new_df = in_table.groupby(group_by_columns).agg(new_agg_rules).reset_index()
        new_table = DataTable(name=table_name, data=new_df)

        op_context.output_tables.append(new_table)
        return True


op_register.register_op(GroupByColumnsOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="group_by_columns", type="list", desc="分组列名") \
    .add_attr(name="agg_rules", type="list", desc="聚合规则") \
    .add_attr(name="table_name", type="str", desc="新表名")
