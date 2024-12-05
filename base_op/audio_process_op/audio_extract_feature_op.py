import os.path

from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AudioExtractFeatureOp(Op):
    """
    【remote】音频特征提取算子，用于提取音频文件的特征，输出音频特征

    Attributes:
        audio_file_column (str): 音频文件列的名称，默认为"audio_file"。
        audio_feature_column (str): 音频特征列的名称，默认为"audio_feature"。

    InputTables:
        material_table: 音频文件所在的表。

    OutputTables:
        material_table: 添加了音频特征的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/audio_extract_feature_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.audio_extract_feature_op import *

        # 创建一个包含音频文件路径的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_need_extract_feature_file':['test_op/test_audio.mp3']
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 AudioExtractFeatureOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        audio_extract_feature_op = AudioExtractFeatureOp(
            name='AudioExtractFeatureOp',
            attrs={
            'audio_file_column': 'audio_need_extract_feature_file',
        })
        success = audio_extract_feature_op.process(op_context)

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
        audio_feature_column = self.attrs.get("audio_feature_column", "audio_feature")

        client = ClientManager().get_client_by_name("AudioFeatureExtractClient")
        material_table[audio_feature_column] = None
        for index, row in material_table.iterrows():
            audio_file: str = row.get(audio_file_column)
            if not audio_file or not os.path.exists(audio_file):
                continue

            audio_feature = client.sync_req(audio_file)
            material_table.at[index, audio_feature_column] = audio_feature

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(AudioExtractFeatureOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_file_column", type="str", desc="音频文件列名") \
    .add_attr(name="audio_feature_column", type="str", desc="音频特征列名")
