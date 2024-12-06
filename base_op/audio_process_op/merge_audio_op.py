from pydub import AudioSegment

from video_graph.common.utils.tools import generate_random_string
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class MergeAudioOp(Op):
    """
    Function:
        音频合并算子，用于合并音频文件列表中的音频文件，输出合并后音频文件路径

    Attributes:
        audio_file_list_column (str): 音频文件列表列的名称，默认为"audio_file_list"。
        target_audio_file_column (str): 目标音频文件列的名称，默认为"target_audio_file"。

    InputTables:
        shot_table: 音频文件列表所在的表。

    OutputTables:
        shot_table: 添加了合并后音频文件的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/merge_audio_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.merge_audio_op import *

        # 创建一个包含两个音频的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_file_need_merge_list':[['test_op/test_merge1.m4a','test_op/test_merge2.m4a']]
            }
        )
        # 设置 OpContext
        #process_id是用于设置存放输出文件的文件夹，request_id和thread_id用于组成文件名前缀，可对比输出表的文件路径查看
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.process_id = "test_op"
        op_context.request_id = "merge"
        op_context.thread_id = thread_id = "audio"
        op_context.input_tables.append(input_table)

        # 实例化 MergeAudioOp 并运行
        merge_audio_op = MergeAudioOp(
            name='MergeAudioOp',
            attrs={
            "audio_file_list_column": "audio_file_need_merge_list",
        })
        success = merge_audio_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")

        #可以将得到的音频文件结合之前的AudioExtractAsrOp算子查看合并后的视频信息是否正确
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        audio_file_list_column = self.attrs.get("audio_file_list_column", "audio_file_list")
        target_audio_file_column = self.attrs.get("target_audio_file_column", "target_audio_file")
        target_audio_file_prefix = f"{op_context.request_id}-{op_context.thread_id}"
        file_directory = f"{op_context.process_id}"

        shot_table[target_audio_file_column] = None
        for index, row in shot_table.iterrows():
            audio_file_list = row.get(audio_file_list_column)
            if len(audio_file_list) == 0:
                continue

            # 过滤掉无效值
            audio_file_list = [audio for audio in audio_file_list if audio]

            combined = AudioSegment.from_file(audio_file_list[0])
            for audio_file in audio_file_list[1:]:
                combined += AudioSegment.from_file(audio_file)

            target_audio_file = f"{file_directory}/{target_audio_file_prefix}-{generate_random_string(5)}.wav"
            combined.export(target_audio_file, format="wav")

            shot_table.at[index, target_audio_file_column] = target_audio_file

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(MergeAudioOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="audio_file_list_column", type="str", desc="音频文件列表列名") \
    .add_attr(name="target_audio_file_column", type="str", desc="目标音频文件列名")
