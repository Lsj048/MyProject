from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class MusicDetectionOp(Op):
    """
    【remote】音乐检测算子，用于检测音频文件中的音乐部分和人声部分，输出音乐检测结果

    Attributes:
        audio_feature_column (str): 音频特征列的名称，默认为"audio_feature"。
        music_detection_resp_column (str): 音乐检测结果列的名称，默认为"music_detection_resp"。
        sing_vocal_part_column (str): 唱歌声部列的名称，默认为"sing_vocal_part"。
        human_vocal_part_column (str): 人声声部列的名称，默认为"human_vocal_part"。

    InputTables:
        material_table: 音频特征所在的表。

    OutputTables:
        material_table: 添加了音乐检测结果的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/music_detection_op.py?ref_type=heads

    Examples:
        #需要先获取audio的feature才能继续使用MusicDetectionOp，所以先调用AudioExtractFeature算子
        from video_graph.ops.base_op.audio_process_op.music_detection_op import *
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

        # 实例化 AudioExtractFeature 并运行，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        audio_extract_feature_op = AudioExtractFeatureOp(
            name='AudioExtractFeatureOp',
            attrs={
            'audio_file_column': 'audio_need_extract_feature_file',
        })
        success = audio_extract_feature_op.process(op_context)

        # 得到输出表，并将插入到op_context.input_tables的表头，继续执行算子，可以打印op_context的输入表头是否有变化
        output_table = op_context.output_tables[0]
        op_context.input_tables.insert(0,output_table)

        # 实例化 MusicDetectionOp 并运行
        music_detection_op = MusicDetectionOp(
            name='MusicDetectionOp',
            attrs={
            "audio_feature_column": "audio_feature"
        })
        success = music_detection_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        audio_feature_column = self.attrs.get("audio_feature_column", "audio_feature")
        music_detection_resp_column = self.attrs.get("music_detection_resp_column", "music_detection_resp")
        sing_vocal_part_column = self.attrs.get("sing_vocal_part_column", "sing_vocal_part")
        human_vocal_part_column = self.attrs.get("human_vocal_part_column", "human_vocal_part")

        client = ClientManager().get_client_by_name("MusicDetectionClient")
        material_table[music_detection_resp_column] = None
        material_table[sing_vocal_part_column] = None
        material_table[human_vocal_part_column] = None
        for index, row in material_table.iterrows():
            audio_feature = row.get(audio_feature_column)
            resp = client.sync_req(audio_feature)
            if resp is None or len(resp.get("duration_rate")) != 3:
                continue

            _, sing_vocal_part, human_vocal_part = resp.get("duration_rate")
            material_table.at[index, music_detection_resp_column] = resp
            material_table.at[index, sing_vocal_part_column] = sing_vocal_part
            material_table.at[index, human_vocal_part_column] = human_vocal_part

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(MusicDetectionOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_feature_column", type="str", desc="音频特征列名") \
    .add_attr(name="music_detection_resp_column", type="str", desc="音乐检测结果列名") \
    .add_attr(name="sing_vocal_part_column", type="str", desc="唱歌部分列名") \
    .add_attr(name="human_vocal_part_column", type="str", desc="人声部分列名")
