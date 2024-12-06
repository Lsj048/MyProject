import os

from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import get_video_base_info
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoBaseInfoOp(Op):
    """
    Function:
        提取视频的基本信息，输出视频的时长、帧率、宽度和高度

    Attributes:
        video_file_column (str): 视频文件路径所在的列名，默认为"video_file_path"
        video_index_column (str): 视频索引列名，默认为"video_index"
        duration_column (str): 视频时长列名，默认为"duration"
        fps_column (str): 视频帧率列名，默认为"fps"
        width_column (str): 视频宽度列名，默认为"width"
        height_column (str): 视频高度列名，默认为"height"

    InputTables:
        material_table: 视频文件所在的表格

    OutputTables:
        material_table: 添加了视频基本信息的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_base_info_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.video_process_op.video_base_info_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable",
            data = {
                "video_file_path": ["test_op/test_video.mp4"],
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        video_base_info_op = VideoBaseInfoOp(
            name="VideoBaseInfoOp",
            attrs={
                "video_file_column": "video_file_path",
                "video_index_column": "file_index",
                "fps_column": "fps",
                "width_column": "width",
                "height_column": "height",
                "duration_column": "duration"
            }
        )

        # 执行算子
        success = video_base_info_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """


    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_file_column = self.attrs.get("video_file_column", "video_file_path")
        video_index_column = self.attrs.get("video_index_column", "video_index")
        duration_column = self.attrs.get("duration_column", "duration")
        fps_column = self.attrs.get("fps_column", "fps")
        width_column = self.attrs.get("width_column", "width")
        height_column = self.attrs.get("height_column", "height")

        status = False
        video_index = 0
        material_table[duration_column] = 0.0
        material_table[fps_column] = 0
        material_table[width_column] = 0
        material_table[height_column] = 0
        for index, row in material_table.iterrows():
            video_index += 1
            video_filename = row.get(video_file_column)
            if not video_filename or not os.path.exists(video_filename):
                logger.error(f"{video_filename} is not exist")
                continue
            duration, fps, width, height = get_video_base_info(video_filename)

            material_table.loc[index, video_index_column] = video_index
            material_table.loc[index, duration_column] = duration
            material_table.loc[index, fps_column] = int(fps)
            material_table.loc[index, width_column] = int(width)
            material_table.loc[index, height_column] = int(height)
            status = True

        op_context.output_tables.append(material_table)
        return status


op_register.register_op(VideoBaseInfoOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_file_column", type="str", desc="视频文件地址列名") \
    .add_attr(name="video_index_column", type="str", desc="视频编号列名") \
    .add_attr(name="duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="fps_column", type="str", desc="fps列名") \
    .add_attr(name="width_column", type="str", desc="视频宽度列名") \
    .add_attr(name="height_column", type="str", desc="视频高度列名")
