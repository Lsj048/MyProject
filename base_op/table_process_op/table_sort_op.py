from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TableSortOp(Op):
    """
    Function:
        表排序算子，支持多列排序、升序/降序排序、是否原地排序

    Attributes:
        column_list (list): 排序的列名列表
        inplace (bool): 是否就地执行，默认为True
        table_name (str): 新表名
        ascending (bool): 是否升序排序，默认为True
        str_to_int (bool): 是否将字符串转换为整数排序，默认为False

    InputTables:
        in_table: 输入表格

    OutputTables:
        in_table or new_table: 排序后的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/table_process_op/table_sort_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.table_process_op.table_sort_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "id": [1,2,3,4],
                "nums":[12,234,13,76]
            }
        )
        display(input_table)

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        table_sort_op = TableSortOp(
            name="TableSortOp",
            attrs={
                "column_list":"nums",
                "inplace":True,
                "table_name":"new_table",
                "ascending":True
            }
        )

        # 执行算子
        success = table_sort_op.process(op_context)

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
        column_list = self.attrs.get("column_list")
        inplace = self.attrs.get("inplace", True)
        table_name = self.attrs.get("table_name")
        ascending = self.attrs.get("ascending", True)
        str_to_int = self.attrs.get("str_to_int", False)

        key_func = None
        if str_to_int:
            key_func = lambda x: x.astype(int)

        if inplace:
            in_table.sort_values(by=column_list, inplace=True, ascending=ascending, key=key_func)
            op_context.output_tables.append(in_table)
        else:
            new_df = in_table.sort_values(by=column_list, inplace=False, ascending=ascending, key=key_func)
            new_table = DataTable(name=table_name, data=new_df)
            op_context.output_tables.append(new_table)

        return True


op_register.register_op(TableSortOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="column_list", type="list", desc="排序的列名列表") \
    .add_attr(name="inplace", type="bool", desc="是否就地执行，默认为True") \
    .add_attr(name="table_name", type="str", desc="新表名") \
    .add_attr(name="ascending", type="bool", desc="是否升序排序，默认为True") \
    .add_attr(name="str_to_int", type="bool", desc="是否将字符串转换为整数排序，默认为False")
