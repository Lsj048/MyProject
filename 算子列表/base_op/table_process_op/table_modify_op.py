from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableModifyOp(Op):
    """
    Function:
        基于pandas的eval接口实现的表修改算子，表达式用法类似于query，但eval可以对列值做操作
        用法参考：https://jakevdp.github.io/PythonDataScienceHandbook/03.12-performance-eval-and-query.html

    Attributes:
        expression (str): 修改表达式
        inplace (bool, optional): 是否在原表上进行操作，默认为 True
        table_name (str, optional): 新表的名称，仅在inplace为False时有效

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table/new_table: 修改后的表格，如果 inplace 为 True，则输出表为原表，否则为新表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_modify_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.table_process_op.table_modify_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        'nums1':["Tom","Jack","Peter"],
        'nums2':[",11 years old",",15 years old",", 19 years old"]
    }
)
display(input_table)
# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子,
#expression为修改表格的表达式
table_modify_op = TableModifyOp(
    name="TableModifyOp",
    attrs={
        "expression": "nums1 + nums2",
        "inplace": False,
        "table_name":"new_table"
    }
)

# 执行算子
success = table_modify_op.process(op_context)

# 检查输出表格
if success:
    output_table = op_context.output_tables[0]
    display(output_table)
else:
    print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        expression = self.attrs.get("expression")
        inplace = self.attrs.get("inplace", True)
        table_name = self.attrs.get("table_name")

        new_df = in_table.eval(expression, inplace=inplace)
        if inplace:
            op_context.output_tables.append(in_table)
        else:
            new_table = DataTable(name=table_name, data=new_df)
            op_context.output_tables.append(new_table)
        return True


op_register.register_op(TableModifyOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="expression", type="str", desc="修改表达式") \
    .add_attr(name="inplace", type="bool", desc="是否就地处理") \
    .add_attr(name="table_name", type="str", desc="新表名")
