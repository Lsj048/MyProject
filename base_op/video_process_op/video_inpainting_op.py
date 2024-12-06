from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoInpaintingOp(Op):
    """
    Function:
        视频字幕擦除算子，使用商业化的inpainting服务进行字幕擦除

    Attributes:
        input:
        video_blob_key_column (str, optional): 视频 BlobKey 所在的列名，默认为 "video_blob_key"
        video_bbox_blob_key_column (str, optional): bbox 文件 BlobKey 所在的列名, 默认为 "video_bbox_blob_key"

        output:
        video_inpainting_status_column (str, optional): 字幕擦除任务状态所在列, 默认为 "video_inpainting_status"
        masked_video_blob_key_column (str, optional): 处理完的视频 BlobKey 所在的列名, 默认为 "masked_video_blob_key"
        output_blob_db (str, optional): 输出视频存放的blob db，默认为 "ad"
        output_blob_table (str, optional): 输出视频存放的blob table，默认为 "nieuwland-material"

    InputTables:
        material_table: 视频BlobKey所在的表格

    OutputTables:
        material_table: 添加了视频字幕擦除结果的表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_process_op/video_inpainting_op.py?ref_type=heads

    Examples:
        #等带作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get('video_blob_key_column', 'video_blob_key')
        video_bbox_blob_key_column = self.attrs.get('video_bbox_blob_key_column', 'video_bbox_blob_key')
        video_inpainting_status_column = self.attrs.get('video_inpainting_status_column', 'video_inpainting_status')
        masked_video_blob_key_column = self.attrs.get('masked_video_blob_key_column', 'masked_video_blob_key')
        output_blob_db = self.attrs.get('output_blob_db', 'ad')
        output_blob_table = self.attrs.get('output_blob_table', 'nieuwland-material')

        video_inpainting_client = ClientManager().get_client_by_name('VideoInpaintingClient')
        for index, row in material_table.iterrows():
            video_blob_key = row.get(video_blob_key_column)
            video_bbox_blob_key = row.get(video_bbox_blob_key_column)
            if str(video_bbox_blob_key) == 'nan':  # 说明该段没有需要擦除的, 复用原始视频 TODO: 把复用 blob key 改成复用本地文件
                material_table.loc[index, masked_video_blob_key_column] = video_blob_key
                material_table.loc[index, video_inpainting_status_column] = True
                continue

            db, table, key = parse_bbs_resource_id(video_blob_key)
            masked_video_blob_key = '_'.join([output_blob_db, output_blob_table, f'mask-subtitle-inpainting-{key}'])

            mask_info = video_inpainting_client.sync_req(op_context.request_id, video_blob_key, video_bbox_blob_key,
                                                         masked_video_blob_key)
            if mask_info is not None and mask_info['status'] == 'SUCCESS':
                material_table.loc[index, masked_video_blob_key_column] = masked_video_blob_key
                material_table.loc[index, video_inpainting_status_column] = True
            else:
                material_table.loc[index, masked_video_blob_key_column] = None
                material_table.loc[index, video_inpainting_status_column] = False

        op_context.output_tables.append(material_table)
        return True


op_register.register_op(VideoInpaintingOp) \
    .add_input(name='material_table', type='DataTable', desc='视频BlobKey所在的表格') \
    .add_output(name='material_table', type='DataTable', desc='添加了视频字幕擦除结果的表格') \
    .add_attr(name='video_blob_key_column', type='str', desc=' 视频 BlobKey 所在的列名') \
    .add_attr(name='video_bbox_blob_key_column', type='str', desc='box 文件 BlobKey 所在的列名') \
    .add_attr(name='video_inpainting_status_column', type='str', desc='字幕擦除任务状态所在列') \
    .add_attr(name='masked_video_blob_key_column', type='str', desc='处理完的视频 BlobKey 所在的列名') \
    .set_parallel(True)
