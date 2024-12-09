from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.parse_asr_resp import parse_photo_mmu_asr_response
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class PhotoAsrDetectMmuOp(Op):
    """
    Function:
        通过 mmu 接口对 photo_id 进行 asr 检测，会优先使用缓存结果

    Attributes:
        photo_id_column (str): photo_id所在的列名，默认为"photo_id"
        asr_texts_column (str): asr文本结果存放的列名，默认为"asr_texts"
        asr_caption_column (str): 带时间的asr文本存放的列名，默认为"tts_caption"

    InputTables:
        video_table: photo_id所在的表格

    OutputTables:
        video_table: 添加了asr_texts和tts_caption的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/photo_asr_detect_mmu_op.py?ref_type=heads

    Examples:
from video_graph.ops.base_op.video_process_op.photo_asr_detect_mmu_op import *

# 创建输入表格
input_table = DataTable(
    name="TestTable",
    data = {
        "photo_id": [144419305964]
    }
)

# 创建操作上下文
op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
op_context.input_tables.append(input_table)

# 配置并实例化算子
photo_asr_detect_mmu_op = PhotoAsrDetectMmuOp(
    name="PhotoAsrDetectMmuOp",
    attrs={
        "photo_id_column": "photo_id",
        "photo_ocr_res_column": "photo_ocr_res"
    }
)

# 执行算子
success = photo_asr_detect_mmu_op.process(op_context)

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
        photo_id_column = self.attrs.get('photo_id_column', 'photo_id')
        asr_texts_column = self.attrs.get('asr_texts_column', 'asr_texts')
        asr_caption_column = self.attrs.get('asr_caption_column', 'tts_caption')

        photo_asr_client = ClientManager().get_client_by_name('PhotoAsrMmuClient')
        video_table[asr_texts_column] = None
        video_table[asr_caption_column] = None
        for index, row in video_table.iterrows():
            photo_id = row.get(photo_id_column)
            resp = photo_asr_client.sync_req(photo_id)

            asr_texts, asr_start_end, asr_caption = parse_photo_mmu_asr_response(resp, photo_id, clear_num=False)

            video_table.at[index, asr_texts_column] = asr_texts
            video_table.at[index, asr_caption_column] = asr_caption

        op_context.output_tables.append(video_table)
        return True


op_register.register_op(PhotoAsrDetectMmuOp) \
    .add_input(name='material_table', type='DataTable', desc='素材表') \
    .add_output(name='material_table', type='DataTable', desc='素材表') \
    .add_attr(name="photo_id_column", type="str", desc="photo_id列名") \
    .add_attr(name="asr_texts_column", type="str", desc="asr文本列名") \
    .add_attr(name="tts_caption_column", type="str", desc="tts caption列名") \
    .set_parallel(True)
