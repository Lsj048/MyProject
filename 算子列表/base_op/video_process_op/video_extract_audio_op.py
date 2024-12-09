import os

from video_graph.common.utils.tools import extract_audio
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoExtractAudioOp(Op):
    """
    Function:
        抽取音频算子，从视频文件中抽取音频文件，可以选择音频类型为wav或mp3

    Attributes:
        video_file_column (str, optional): 视频文件路径所在的列名，默认为"video_file_path"。
        audio_file_column (str, optional): 音频文件路径保存的列名，默认为"audio_file_path"。
        audio_type (str, optional): 音频类型，默认为“wav”。

    InputTables:
        material_table: 视频文件所在的表格

    OutputTables:
        material_table: 添加了音频文件路径的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_extract_audio_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.video_process_op.video_extract_audio_op import *

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
video_extract_audio_op = VideoExtractAudioOp(
    name="VideoExtractAudioOp",
    attrs={
        "video_file_column":"video_file_path",
        "audio_file_column":"audio_file_path",
        "audio_type":"wav"
    }
)

# 执行算子
success = video_extract_audio_op.process(op_context)

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
        audio_file_column = self.attrs.get("audio_file_column", "audio_file_path")
        audio_type = self.attrs.get("audio_type", "wav")

        material_table[audio_file_column] = None
        for index, row in material_table.iterrows():
            video_file = row.get(video_file_column)
            if not video_file or not os.path.exists(video_file):
                continue

            basename, extension = os.path.splitext(video_file)
            audio_filename = f"{basename}.{audio_type}"
            if not extract_audio(video_file, audio_filename, audio_type):
                continue

            material_table.loc[index, audio_file_column] = audio_filename

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoExtractAudioOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="video_file_column", type="str", desc="视频文件地址列名") \
    .add_attr(name="audio_file_column", type="str", desc="音频文件地址列名") \
    .set_parallel(True)
