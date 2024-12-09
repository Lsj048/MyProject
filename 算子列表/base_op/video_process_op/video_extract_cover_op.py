import os

import cv2

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoExtractCoverOp(Op):
    """
    Function:
        提取视频封面，输出视频的第一帧作为封面

    Attributes:
        video_file_column (str, optional): 视频文件路径所在的列名，默认为"video_file_path"。
        cover_file_column (str, optional): 封面文件路径保存的列名，默认为"cover_file_path"。

    InputTables:
        material_table: 视频文件所在的表格

    OutputTables:
        material_table: 添加了封面文件路径的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_extract_cover_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.video_process_op.video_extract_cover_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "video_file_path":["test_op/file.mp4"]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
video_extract_cover_op = VideoExtractCoverOp(
    name="VideoExtractCoverOp",
    attrs={
        "video_file_column":"video_file_path",
        "cover_file_column":"cover_file_path",
    }
)

# 执行算子
success = video_extract_cover_op.process(op_context)

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
        cover_file_column = self.attrs.get("cover_file_column", "cover_file_path")
        file_directory = f"{op_context.process_id}"

        status = False
        material_table[cover_file_column] = None
        for index, row in material_table.iterrows():
            video_file = row.get(video_file_column)
            if not os.path.exists(video_file):
                continue
            cap = cv2.VideoCapture(video_file)
            success, first_frame = cap.read()
            if success:
                video_basename = os.path.basename(video_file)
                video_prefix = os.path.splitext(video_basename)[0]
                cover_file_path = os.path.join(file_directory, f"{video_prefix}.jpg")
                cv2.imwrite(cover_file_path, first_frame)
                material_table.loc[index, cover_file_column] = cover_file_path
                status = True

        op_context.output_tables.append(material_table)
        return status


op_register.register_op(VideoExtractCoverOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_file_column", type="str", desc="视频文件地址列名") \
    .add_attr(name="cover_file_column", type="str", desc="封面文件地址列名")
