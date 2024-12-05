import os.path

from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import audio_normalization
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AudioNormalizationOp(Op):
    """
    【local】音频标准化算子，使其达到标准的音量强度，输出标准化后音频文件路径

    Attributes:
        audio_file_column (str): 音频文件路径所在列的名称，默认为"audio_file_path"。
        output_file_column (str): 标准化后音频文件路径所在列的名称，默认为"normalized_audio_file"。

    InputTables:
        shot_table: 音频文件所在的表。

    OutputTables:
        shot_table: 添加了标准化后音频文件路径的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/audio_normalization_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.audio_normalization_op import *

        # 创建一个包含音频文件路径的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_need_normalization_file':['test_op/test_audio.mp3']
            }
        )

        # 设置 OpContext
        #process_id是存放输出文件的文件夹
        process_id = "test_op"
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.process_id = process_id
        op_context.input_tables.append(input_table)

        # 实例化 AudioNormaliazationOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        audio_normalization_op = AudioNormalizationOp(
            name='AudioNormalizationOp',
            attrs={
            'audio_file_column': 'audio_need_normalization_file',
        })
        success = audio_normalization_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        audio_file_column = self.attrs.get("audio_file_column", "audio_file_path")
        output_file_column = self.attrs.get("output_file_column", "normalized_audio_file")
        file_directory = f"{op_context.process_id}"

        shot_table[output_file_column] = None
        for index, row in shot_table.iterrows():
            audio_file = row.get(audio_file_column)
            if not audio_file or not os.path.exists(audio_file):
                logger.error(f"audio file error, audio_file:{audio_file}")
                continue

            basename = os.path.basename(audio_file)
            basename_prefix, extension = os.path.splitext(basename)
            output_file = os.path.join(file_directory, f"audio-norm-{basename_prefix}.wav")
            if not audio_normalization(audio_file, output_file):
                logger.error(f"audio normalization failed, audio_file:{audio_file}, output_file:{output_file}")
                continue

            shot_table.loc[index, output_file_column] = output_file

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(AudioNormalizationOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_file_column", type="str", desc="音频文件列名") \
    .add_attr(name="output_file_column", type="str", desc="输出文件列名")
