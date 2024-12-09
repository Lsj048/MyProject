from video_graph import logger
from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.tools import build_bbs_resource_id, get_timestamp
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VtuberHumanRenderOp(Op):
    """
    Function:
        虚拟人渲染算子，可以指定虚拟人 id 和 tts 资源，输出虚拟人视频的blob key

    Attributes:
        tts_blob_key_column (str): TTS Blob Key 列名，默认为"tts_blob_key"。
        vtuber_id_column (str): 虚拟人 ID 列名，默认为"vtuber_id"。
        vtuber_video_db (str): 虚拟人视频数据库，默认为"ad"。
        vtuber_video_table (str): 虚拟人视频表格，默认为"nieuwland-material"。
        vtuber_video_blob_key_column (str): 虚拟人视频 Blob Key 列名，默认为"vtuber_video_blob_key"。
        vtuber_response_column (str): 虚拟人响应列名，默认为"vtuber_response"。
        is_real_scene_vtuber_column (str): 是否实景数字人列名，默认为“is_real_scene_vtuber_column”。

    InputTables:
        shot_table: 输入表格。

    OutputTables:
        shot_table: 添加了虚拟人视频 Blob Key 和虚拟人响应的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/vtuber_human_op/vtuber_human_render_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        tts_blob_key_column = self.attrs.get("tts_blob_key_column", "tts_blob_key")
        vtuber_id_column = self.attrs.get("vtuber_id_column", "vtuber_id")
        vtuber_video_db = self.attrs.get("vtuber_video_db", "ad")
        vtuber_video_table = self.attrs.get("vtuber_video_table", "nieuwland-material")
        vtuber_video_blob_key_column = self.attrs.get("vtuber_video_blob_key_column", "vtuber_video_blob_key")
        vtuber_response_column = self.attrs.get("vtuber_response_column", "vtuber_response")
        is_real_scene_vtuber_column = self.attrs.get("is_real_scene_vtuber_column", "is_real_scene_vtuber")
        output_video_key_prefix = f"{op_context.request_id}-{get_timestamp()}"

        vtuber_human_client = ClientManager().get_client_by_name("VtuberHumanClient")
        shot_table[vtuber_video_blob_key_column] = None
        shot_table[vtuber_response_column] = None
        for index, row in shot_table.iterrows():
            tts_blob_key = row.get(tts_blob_key_column)
            vtuber_id = row.get(vtuber_id_column)
            is_real_scene_vtuber = row.get(is_real_scene_vtuber_column, False)
            vtuber_video_key = f"{output_video_key_prefix}-{index}.mp4"
            vtuber_video_resource_id = build_bbs_resource_id((vtuber_video_db, vtuber_video_table, vtuber_video_key))
            request_id = f"{output_video_key_prefix}-{index}"
            resp = vtuber_human_client.sync_req(request_id, vtuber_id, tts_blob_key, vtuber_video_resource_id,
                                                is_real_scene_vtuber=is_real_scene_vtuber)
            if resp is None or resp.get("error_code") != 0:
                self.fail_reason = f"虚拟人渲染失败: {resp.get('msg') if resp else 'timeout'}"
                self.trace_log.update({"fail_reason": self.fail_reason})
                logger.info(f"vtuber video resp is error, resp:{resp}")
                return False

            shot_table.at[index, vtuber_video_blob_key_column] = vtuber_video_resource_id
            shot_table.at[index, vtuber_response_column] = resp

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(VtuberHumanRenderOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .set_parallel(True)
