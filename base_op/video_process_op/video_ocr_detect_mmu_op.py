from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoOcrDetectMmuOp(Op):
    """
    Function:
        通过 mmu 接口进行 ocr 检测

    Attributes:
        video_blob_key_column (str): 视频BlobKey所在的列名，默认为"video_blob_key"
        video_ocr_res_column (str): 视频OCR结果列名，默认为"ocr"

    InputTables:
        video_table: 视频BlobKey所在的表格

    OutputTables:
        video_table: 添加了视频OCR结果的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_ocr_detect_mmu_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.video_process_op.video_ocr_detect_mmu_op import *

        # 创建输入表格，blob_key通过FileUploaderOp算子获得
        input_table = DataTable(
            name="TestTable",
            data = {
                "video_blob_key": ["ad_nieuwland-material_test_video_file.mp4"]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        video_ocr_detect_mmu_op = VideoOcrDetectMmuOp(
            name="VideoOcrDetectMmuOp",
            attrs={
                "video_blob_key_column": "video_blob_key",
                "video_ocr_res_column": "ocr_result"
            }
        )

        # 执行算子
        success = video_ocr_detect_mmu_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute_when_skip(self, op_context: OpContext):
        op_context.output_tables.append(op_context.input_tables[0])
        return True

    def compute(self, op_context: OpContext) -> bool:
        video_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get('video_blob_key_column', 'video_blob_key')
        video_ocr_res_column = self.attrs.get('video_ocr_res_column', 'ocr')

        video_ocr_client = ClientManager().get_client_by_name('VideoOcrMmuClient')
        video_table[video_ocr_res_column] = None
        for index, row in video_table.iterrows():
            video_blob_key = row.get(video_blob_key_column)
            resp = video_ocr_client.sync_req(video_blob_key)
            video_table.at[index, video_ocr_res_column] = resp

        op_context.output_tables.append(video_table)
        return True


op_register.register_op(VideoOcrDetectMmuOp) \
    .add_input(name='material_table', type='DataTable', desc='素材表') \
    .add_output(name='material_table', type='DataTable', desc='素材表') \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blobstore地址列名") \
    .add_attr(name="video_ocr_res_column", type="str", desc="视频ocr检测结果列名") \
    .set_parallel(True)
