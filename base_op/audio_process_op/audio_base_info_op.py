import os.path

from pydub import AudioSegment

from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class AudioBaseInfoOp(Op):
    """
    Function:
        音频基本信息获取算子，输出音频时长

    Attributes:
        audio_file_column (str): 音频文件所在列的名称，默认为"audio_file"。
        duration_column (str): 音频时长列的名称，默认为"duration"。

    InputTables:
        material_table: 音频文件所在的表。
 
    OutputTables:
        material_table: 添加了音频基本信息的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/audio_base_info_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.audio_base_info_op import *
        # 创建一个包含音频文件路径的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_need_get_info_file':['test_op/test_audio.mp3']
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 AudioBaseInfoOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        audio_op = AudioBaseInfoOp(
            name='AudioBaseInfoOp',
            attrs={
            'audio_file_column': 'audio_need_get_info_file',
        })
        success = audio_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        audio_file_column = self.attrs.get("audio_file_column", "audio_file")
        duration_column = self.attrs.get("duration_column", "duration")

        material_table[duration_column] = 0.0
        for index, row in material_table.iterrows():
            audio_file = row.get(audio_file_column)
            if not audio_file or not os.path.exists(audio_file):
                continue

            audio_file = AudioSegment.from_file(audio_file)
            audio_duration = audio_file.duration_seconds

            material_table.loc[index, duration_column] = audio_duration

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(AudioBaseInfoOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_file_column", type="str", desc="音频文件列名") \
    .add_attr(name="duration_column", type="str", desc="音频时长列名")
