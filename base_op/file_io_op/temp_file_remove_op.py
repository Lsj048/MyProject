import os
import shutil

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TempFileRemoveOp(Op):
    """
    Function:
        临时文件删除算子，用于删除指定的文件或目录

    Attributes:
        file_path_columns (str or List[str]): 指定要删除的文件路径列名或列名列表。

    InputTables:
        in_table: 输入表格。

    OutputTables:
        in_table: 未做修改的输入表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/file_io_op/temp_file_remove_op.py?ref_type=heads

    Examples:
        import os
        from video_graph.ops.base_op.file_io_op.temp_file_remove_op import *

        # 创建删除多列情况下的输入表
        input_table = DataTable(
            name="testTable",
            data={
                'need_delete_path1': ['test_op/test_delete_file.jpg'],
                'need_delete_path2':['test_op/test_delete_dir']
        })

        #查看输入表的结构
        display(input_table)

        # 设置算子属性(删除多列的情况)，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        temp_file_remove_op = TempFileRemoveOp(
            name='TempFileRemoveOp',
            attrs={
            'file_path_columns': ['need_delete_path1','need_delete_path2']
        })

        # 创建算子执行上下文
        op_context = OpContext("test_graph", request_tag="test_tag", request_id="test_id")
        op_context.input_tables = [input_table]

        # 执行算子
        result = temp_file_remove_op.process(op_context)

        # 验证结果
        assert not os.path.exists('test_op/test_delete_file.jpg'), "Temp file should be removed by the operator."
        assert not os.path.exists('test_op/test_delete_dir'), "Temp dir should be removed by the operator."

        print("TempFileRemoveOp test passed successfully!")
    """

    def compute(self, op_context: OpContext) -> bool:
        in_table: DataTable = op_context.input_tables[0]
        file_path_columns = self.attrs.get("file_path_columns", "file_path")
        if isinstance(file_path_columns, str):
            file_path_columns = [file_path_columns]

        for index, row in in_table.iterrows():
            for file_path_column in file_path_columns:
                file_path = row.get(file_path_column)
                if not file_path:
                    continue
                if os.path.isfile(file_path):
                    os.remove(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

        op_context.output_tables.append(in_table)
        return True


op_register.register_op(TempFileRemoveOp) \
    .add_input(name="any_table", type="DataTable", desc="placeholder") \
    .add_output(name="any_table", type="DataTable", desc="placeholder") \
    .add_attr(name="file_path_columns", type="list/str", desc="文件地址列名")
