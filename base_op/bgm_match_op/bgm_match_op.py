import random

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.tools import build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class BgmMatchOp(Op):
    """
    Function:
        音乐匹配算子，用于为视频列表匹配BGM，如果匹配不到则随机选择BGM，如果匹配到多个BGM则随机选择一个

    Attributes:
        video_list_column (str): 视频列表列名，默认为"video_list"。
        forced_bgm_blob_key (str): 强制指定 BGM Blob Key，默认为None。
        bgm_blob_key_column (str): BGM Blob Key 列名，默认为"bgm_blob_key"。
        bgm_blob_key_list_column (str): BGM Blob Key 列表列名，默认为"bgm_blob_key_list"。
        random_bgm_list (list[str]): 随机BGM列表，默认为[
                "BGM_retrieval/music_foder/3527242720-no_vocals-47.9.mp3",
                "BGM_retrieval/music_foder/3524459843-no_vocals-103.7.mp3",
                "BGM_retrieval/music_foder/9365387194-no_vocals-16.4.mp3",
                "BGM_retrieval/music_foder/6240562188-no_vocals-1.0.mp3",
                "BGM_retrieval/music_foder/14707685787-no_vocals-1.4.mp3"
            ]。

    InputTables:
        shot_table: 视频列表所在的表。

    OutputTables:
        shot_table: 添加了BGM Blob Key 的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/bgm_match_op/bgm_match_op.py?ref_type=heads

    Examples：
        #待作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        video_list_column = self.attrs.get("video_list_column", "video_list")
        forced_bgm_blob_key = self.attrs.get("forced_bgm_blob_key", None)
        bgm_blob_key_column = self.attrs.get("bgm_blob_key_column", "bgm_blob_key")
        bgm_blob_key_list_column = self.attrs.get("bgm_blob_key_list_column", "bgm_blob_key_list")
        random_bgm_list = self.attrs.get("random_bgm_list", [
            "BGM_retrieval/music_foder/3527242720-no_vocals-47.9.mp3",
            "BGM_retrieval/music_foder/3524459843-no_vocals-103.7.mp3",
            "BGM_retrieval/music_foder/9365387194-no_vocals-16.4.mp3",
            "BGM_retrieval/music_foder/6240562188-no_vocals-1.0.mp3",
            "BGM_retrieval/music_foder/14707685787-no_vocals-1.4.mp3"
        ])

        # 强制设置特定的 bgm
        if forced_bgm_blob_key is not None:
            shot_table[bgm_blob_key_column] = [forced_bgm_blob_key] * len(shot_table)
            op_context.output_tables.append(shot_table)
            return True

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        bgm_retrieval_cfg: dict = kconf_params["bgm_retrieval_cfg"]
        no_vocal = bgm_retrieval_cfg["no_vocal"]

        bgm_retrieval_client = ClientManager().get_client_by_name("BgmRetrievalClient")
        shot_table[bgm_blob_key_column] = None
        shot_table[bgm_blob_key_list_column] = None
        for index, row in shot_table.iterrows():
            video_list = row.get(video_list_column)
            if isinstance(video_list, tuple):
                video_list = [video_list]
            music_ids = bgm_retrieval_client.sync_req(video_list, no_vocal)

            if music_ids:
                music_id = random.choice(music_ids)
                music_blob_key = build_bbs_resource_id(["ad", "nieuwland-material", music_id])
            else:
                music_id = random.choice(random_bgm_list)
                music_blob_key = build_bbs_resource_id(["ad", "nieuwland-material", music_id])
            shot_table.at[index, bgm_blob_key_column] = music_blob_key
            if music_ids:
                shot_table.at[index, bgm_blob_key_list_column] = [
                    build_bbs_resource_id(["ad", "nieuwland-material", music_id]) for music_id in music_ids
                ]

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(BgmMatchOp) \
    .add_input(name="shot_table", type="DataTable", desc="placeholder") \
    .add_output(name="shot_table", type="DataTable", desc="placeholder") \
    .add_attr(name="video_list_column", type="str", desc="视频列表列名") \
    .add_attr(name="forced_bgm_blob_key",type="str",desc="强制指定bgm的 Blob Key") \
    .add_attr(name="bgm_blob_key_list_column",type="str", desc="bgm Blob Key 列表列名") \
    .add_attr(name="bgm_blob_key_column", type="str", desc="音乐blob key结果列名") \
    .add_attr(name="random_bgm_list", type="list", desc="随机bgm列表")
