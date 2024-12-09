from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VtuberHumanBboxOp(Op):
    """
    Function:
        计算虚拟人bbox算子

    Attributes:
        vtuber_id_column (str): 虚拟人 ID 列名，默认为"vtuber_id"。
        vtuber_video_blob_key_column (str): 虚拟人视频 Blob Key 列名，默认为"vtuber_video_blob_key"。
        vtuber_video_bbox_column (str): 虚拟人视频 bbox 列名，默认为"vtuber_video_bbox"。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 添加了虚拟人视频 bbox 的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/vtuber_human_op/vtuber_human_bbox_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        vtuber_id_column = self.attrs.get("vtuber_id_column", "vtuber_id")
        vtuber_video_blob_key_column = self.attrs.get("vtuber_video_blob_key_column", "vtuber_video_blob_key")
        vtuber_video_bbox_column = self.attrs.get("vtuber_video_bbox_column", "vtuber_video_bbox")

        request_id = op_context.request_id
        vtuber_human_bbox_client = ClientManager().get_client_by_name("VtuberHumanBboxClient")
        shot_table[vtuber_video_bbox_column] = None
        for index, row in shot_table.iterrows():
            vtuber_id = row.get(vtuber_id_column)
            vtuber_video_blob_key = row.get(vtuber_video_blob_key_column)
            resp = vtuber_human_bbox_client.sync_req(request_id, vtuber_id, vtuber_video_blob_key)

            vtuber_human_bbox = [0, 0, 1, 1]
            if resp and resp.get("status"):
                vtuber_human_bbox = resp.get("bbox")
                vtuber_human_bbox[0] /= 1080
                vtuber_human_bbox[1] /= 1920
                vtuber_human_bbox[2] /= 1080
                vtuber_human_bbox[3] /= 1920

            shot_table.at[index, vtuber_video_bbox_column] = vtuber_human_bbox

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(VtuberHumanBboxOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表")
