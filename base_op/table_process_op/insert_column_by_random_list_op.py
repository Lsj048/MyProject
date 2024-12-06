import random

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class InsertColumnByRandomListOp(Op):
    """
    Function:
        从随机列表中添加列，随机选择列表中的值填充新列

    Attributes:
        column_name (str): 列名
        random_list (list): 随机列表

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/insert_column_by_random_list_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.insert_column_by_random_list_op import *

        # 创建一个输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'exist':[1,2,3,4]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        insert_column_op = InsertColumnByRandomListOp(
            name="InsertColumnByRandomListOp",
            attrs={
                "column_name": "new_generative_column",
                "random_list": [100,111]
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

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        column_name = self.attrs.get("column_name")
        random_list = self.attrs.get("random_list")

        in_table[column_name] = None
        for index, row in in_table.iterrows():
            real_random_list = random_list
            if isinstance(random_list, str):
                random_list_data = row.get(random_list)
                if isinstance(random_list_data, list):
                    real_random_list = random_list_data
                elif isinstance(random_list_data, dict):
                    real_random_list = list(random_list_data.keys())
            column_value = random.choice(real_random_list)
            in_table.at[index, column_name] = column_value

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(InsertColumnByRandomListOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="column_name", type="str", desc="新列名") \
    .add_attr(name="random_list", type="list", desc="随机列表")
