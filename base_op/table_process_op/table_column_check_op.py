from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableColumnCheckOp(Op):
    """
    Function:
        过滤掉列值异常（None/NaN）的行

    Args:
        column_list (list): 列名列表，用于指定需要检查的列
        inplace (bool, optional): 是否在原表上进行操作，默认为True
        table_name (str, optional): 新表的名称，仅在inplace为False时有效

    InputTables:
        in_table: 输入表

    OutputTables:
        过滤掉异常值的新表，如果 inplace 为 True，则输出表为原表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_column_check_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_column_check_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                'id':[1,2,3,4],
                'nums':[12,23,35,None]
            }
        )
        display(input_table)
        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子,
        #column_list存储要清除None/NaN值的列名列表
        #inplace标识是否在原表上进行操作，table_name仅在inpalce为false的时候才起作用
        table_column_check_op = TableColumnCheckOp(
            name="TableColumnCheckOp",
            attrs={
                "column_list": ["nums"],
                "inplace": True,
                "table_name":"new_table"
            }
        )

        # 执行算子
        success = table_column_check_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        column_list = self.attrs.get("column_list")
        inplace = self.attrs.get("inplace", True)
        table_name = self.attrs.get("table_name")

        new_df = in_table.dropna(subset=column_list, inplace=inplace)
        if inplace:
            op_context.output_tables.append(in_table)
        else:
            new_table = DataTable(name=table_name, data=new_df)
            op_context.output_tables.append(new_table)
        return True


op_register.register_op(TableColumnCheckOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="column_list", type="str", desc="要检查的列名list") \
    .add_attr(name="inplace", type="str", desc="是否就地执行") \
    .add_attr(name="table_name", type="str", desc="新表名")
