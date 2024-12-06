from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class AudioExtractAsrOp(Op):
    """
    Function:
        抽取音频ASR算子，调用服务接口获取音频ASR文本，输出ASR文本和ASR字幕

    Attributes:
        audio_file_column (str): 音频文件所在列的名称，默认为"audio_file"。
        asr_column (str): 存储ASR文本的列的名称，默认为"asr"。
        asr_caption_column (str): 存储ASR的文本、开始时间、结束时间的列的名称，默认为"asr_caption"。
        server_version (str): 请求的服务版本，默认为"v2"

    InputTables:
        shot_table: 音频文件所在的表。

    OutputTables:
        shot_table: 添加了ASR文本的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/audio_extract_asr_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.audio_extract_asr_op import *

        # 创建一个包含音频文件路径的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_need_get_asr_file':['test_op/test_audio.mp3']
            }
        )

        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 AudioBaseInfoOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        audio_extract_asr_op = AudioExtractAsrOp(
            name='AudioExtractAsrOp',
            attrs={
            'audio_file_column': 'audio_need_get_asr_file',
        })
        success = audio_extract_asr_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        audio_file_column = self.attrs.get("audio_file_column", "audio_file")
        asr_column = self.attrs.get("asr_column", "asr")
        asr_caption_column = self.attrs.get("asr_caption_column", "asr_caption")
        server_version = self.attrs.get("server_version", "v2")

        video_asr_client = ClientManager().get_client_by_name("VideoAsrClient")
        video_asr_v2_client = ClientManager().get_client_by_name("VideoAsrV2Client")
        video_asr_v3_client = ClientManager().get_client_by_name("VideoAsrV3Client")
        shot_table[asr_column] = None
        shot_table[asr_caption_column] = None
        for index, row in shot_table.iterrows():
            audio_file = row.get(audio_file_column)
            if server_version == "v1":
                asr_text, asr_caption = video_asr_client.sync_req(op_context.request_id, audio_file)
            elif server_version == "v2":
                asr_text, asr_caption = video_asr_v2_client.sync_req(op_context.request_id, audio_file)
            else:
                asr_text, asr_caption = video_asr_v3_client.sync_req(op_context.request_id, audio_file)
            if asr_text:
                video_asr_result = '<SEP>'.join(asr_text)
                shot_table.at[index, asr_column] = video_asr_result
                shot_table.at[index, asr_caption_column] = asr_caption

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(AudioExtractAsrOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_file_column", type="str", desc="音频文件地址列名") \
    .add_attr(name="asr_column", type="str", desc="asr结果列名") \
    .add_attr(name="asr_caption_column", type="list", desc="asr（文本、开始时间、结束时间）列表的列名") \
    .add_attr(name="server_version",type="str",desc="请求的服务版本") \
    .set_parallel(True)
