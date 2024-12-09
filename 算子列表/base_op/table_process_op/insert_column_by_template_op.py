from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class InsertColumnByTemplateOp(Op):
    """
    Function:
        基于模版插入新列算子，支持多列值嵌入的模版字符串

    Attributes:
        template (str): 模版字符串
        new_column_name (str): 新列名
        column_name_1 (str): 模版列列名1
        column_name_2 (str): 模版列列名2
        column_name_3 (str): 模版列列名3

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table: 加入新列后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/insert_column_by_template_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.insert_column_by_template_op import *

# 创建一个输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        'year':[2001,2002,2003,2004],
        'month':[1,2,3,4],
        'day':[5,6,7,8]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
insert_column_op = InsertColumnByTemplateOp(
    name="InsertColumnByTemplateOp",
    attrs={
        "template": "{}年{}月{}日",
        "new_column_name": "new_column",
        "column_name_1": "year",
        "column_name_2": "month",
        "column_name_3": "day"
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
        template = self.attrs.get("template")
        new_column_name = self.attrs.get("new_column_name")
        column_name_1 = self.attrs.get("column_name_1")
        column_name_2 = self.attrs.get("column_name_2")
        column_name_3 = self.attrs.get("column_name_3")

        in_table[new_column_name] = None
        for index, row in in_table.iterrows():
            column_1 = row.get(column_name_1)
            column_2 = row.get(column_name_2)
            column_3 = row.get(column_name_3)

            new_column_value = template.format(column_1, column_2, column_3)
            in_table.at[index, new_column_name] = new_column_value

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(InsertColumnByTemplateOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="template", type="str", desc="模板") \
    .add_attr(name="new_column_name", type="str", desc="新列名") \
    .add_attr(name="column_name_1", type="str", desc="模版列列名1") \
    .add_attr(name="column_name_2", type="str", desc="模版列列名2") \
    .add_attr(name="column_name_3", type="str", desc="模版列列名3")
