import os

from PIL import Image

from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext

class GetImageSizeOp(Op):
    """
    【local】获取图片宽高算子，用于获取图片的宽度和高度

    Attributes:
        image_file_path_column (str): 图片文件路径列名，默认为"image_file_path"。
        width_column (str): 图片宽度列名，默认为"width"。
        height_column (str): 图片高度列名，默认为"height"。

    InputTables:
        image_table: 图片表格。

    OutputTables:
        image_table: 添加了图片宽高的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/image_process_op/get_image_size_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.image_process_op.get_image_size_op import *

        # 创建一个包含图片文件的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'image_file_need_get_size':['test_op/test_picture.jpg']
            }
        )
        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 GetImageSizeOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        get_image_size_op = GetImageSizeOp(
            name='VocalSplitOp',
            attrs={
            "image_file_path_column": "image_file_need_get_size",
        })
        success = get_image_size_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """
    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        image_table: DataTable = op_context.input_tables[0]
        image_file_path_column = self.attrs.get("image_file_path_column", "image_file_path")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")

        image_table[width_column] = 0
        image_table[height_column] = 0
        for index, row in image_table.iterrows():
            image_file = row.get(image_file_path_column)
            if image_file and os.path.exists(image_file):
                with Image.open(image_file) as img:
                    width, height = img.size
                    image_table.at[index, width_column] = width
                    image_table.at[index, height_column] = height

        op_context.output_tables.append(image_table)
        return True


op_register.register_op(GetImageSizeOp) \
    .add_input(name="image_table", type="DataTable", desc="图片表") \
    .add_output(name="image_table", type="DataTable", desc="图片表") \
    .add_attr(name="image_file_path_column", type="str", desc="图片文件路径列名") \
    .add_attr(name="width_column", type="str", desc="图片宽度列名") \
    .add_attr(name="height_column", type="str", desc="图片高度列名")