from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.tools import build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext

class VocalSplitOp(Op):
    """
    Function:
        人声分离算子，用于分离出音频文件中的人声部分，输出人声分离后的音频blob资源

    Attributes:
        audio_blob_key_column (str): 音频BlobKey列的名称，默认为"audio_blob_key"。
        vocal_part_blob_key_column (str): 人声部分BlobKey列的名称，默认为"vocal_part_blob_key"。
        vocal_split_resp_column (str): 人声分离结果列的名称，默认为"vocal_split_resp"。

    InputTables:
        material_table: 音频BlobKey所在的表。

    OutputTables:
        material_table: 添加了人声分离结果的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/vocal_split_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.vocal_split_op import *
        #如果想要测试自己的文文件，audio_blob_key可以先使用FileUploaderOp算子获取，再进行实验

        # 创建一个包含audio_blob_key的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'audio_blob_key':['ad_nieuwland-material_test_audio.mp3']
            }
        )
        # 设置 OpContext
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.input_tables.append(input_table)

        # 实例化 VocalSplitOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        vocal_split_op = VocalSplitOp(
            name='VocalSplitOp',
            attrs={
            "audio_blob_key_column": "audio_blob_key",
        })
        success = vocal_split_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")

        #根据得到的vocal_part_blob_key，可以使用FileDownloadOp算子下载到本地检查
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        audio_blob_key_column = self.attrs.get("audio_blob_key_column", "audio_blob_key")
        vocal_part_blob_key_column = self.attrs.get("vocal_part_blob_key_column", "vocal_part_blob_key")
        vocal_split_resp_column = self.attrs.get("vocal_split_resp_column", "vocal_split_resp")

        client = ClientManager().get_client_by_name("VocalSplitClient")
        material_table[vocal_part_blob_key_column] = None
        material_table[vocal_split_resp_column] = None
        for index, row in material_table.iterrows():
            audio_blob_key = row.get(audio_blob_key_column)
            resp = client.sync_req(audio_blob_key)
            if resp is None:
                continue

            material_table.at[index, vocal_split_resp_column] = resp
            if resp.status == "SUCCESS":
                vocal_part_blob_key = build_bbs_resource_id(resp.res.vocal_part)
                material_table.at[index, vocal_part_blob_key_column] = vocal_part_blob_key

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VocalSplitOp) \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="material_table", type="DataTable", desc="素材表") \
    .add_attr(name="audio_blob_key_column", type="str", desc="音频blobstore地址列名") \
    .add_attr(name="vocal_part_blob_key_column", type="str", desc="人声部分blobstore地址列名") \
    .add_attr(name="vocal_split_resp_column", type="str", desc="人声分离结果列名")