from pydub import AudioSegment

from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class GenerateSilentAudioOp(Op):
    """
    Function:
        生成静音音频算子，用于生成指定时长的静音音频文件，输出静音音频文件路径

    Attributes:
        silent_audio_duration_column (str): 静音音频时长列的名称，默认为"audio_duration"。
        silent_audio_file_column (str): 静音音频文件列的名称，默认为"silent_audio_file"。

    InputTables:
        shot_table: 音频文件所在的表。

    OutputTables:
        shot_table: 添加了静音音频文件的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/generate_silent_audio_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.generate_silent_audio_op import *

        # 创建一个包含音频时长的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_duration':[5]
            }
        )
        # 设置 OpContext
        #process_id是用于设置存放输出文件的文件夹，request_id和thread_id用于组成文件名前缀，可对比输出表的文件路径查看
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.process_id = "test_op"
        op_context.request_id = "generate"
        op_context.thread_id = thread_id = "silence"
        op_context.input_tables.append(input_table)

        # 实例化 GenerateSilentAudioOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        generate_silent_audio_op = GenerateSilentAudioOp(
            name='GenerateSilentAudioOp',
            attrs={
            "silent_audio_duration_column": "audio_duration",
        })
        success = generate_silent_audio_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")

        #可以将得到的音频文件结合之前的AudioBaseInfoOp算子查看信息是否正确
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        silent_audio_duration_column = self.attrs.get("silent_audio_duration_column", "audio_duration")
        silent_audio_file_column = self.attrs.get("silent_audio_file_column", "silent_audio_file")
        silent_audio_file_prefix = f"{op_context.request_id}-{op_context.thread_id}"
        file_directory = f"{op_context.process_id}"

        shot_table[silent_audio_file_column] = None
        for index, row in shot_table.iterrows():
            silent_audio_duration = row.get(silent_audio_duration_column)
            silent_audio = AudioSegment.silent(duration=1000 * silent_audio_duration)
            silent_audio_file = f"{file_directory}/{silent_audio_file_prefix}-silent-audio-{int(silent_audio_duration)}.wav"
            silent_audio.export(silent_audio_file, format="wav")

            shot_table.at[index, silent_audio_file_column] = silent_audio_file

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(GenerateSilentAudioOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="silent_audio_duration_column", type="str", desc="静音音频时长列名") \
    .add_attr(name="silent_audio_file_column", type="str", desc="静音音频文件列名")
