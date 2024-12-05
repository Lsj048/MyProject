import random

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.tools import build_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class BgmMatchOpV2(Op):
    """
    【remote】音乐匹配V2算子，相比BgmMatchOp，增加了二级行业和BGM版本的控制

    Attributes:
        video_list_column (str): 视频列表列名，默认为"video_list"。
        forced_bgm_blob_key (str): 强制指定 BGM Blob Key，默认为None。
        bgm_blob_key_column (str): BGM Blob Key 列名，默认为"bgm_blob_key"。
        bgm_blob_key_list_column (str): BGM Blob Key 列表列名，默认为"bgm_blob_key_list"。
        second_industry_name_column (str): 二级行业列名，默认为"second_industry_name"。
        bgm_version_column (str): BGM版本列名，默认为"bgm_version"。
        random_bgm_list (list[str]): 随机BGM列表，默认为[
                "BGM_retrieval/music_foder/3527242720-no_vocals-47.9.mp3",
                "BGM_retrieval/music_foder/3524459843-no_vocals-103.7.mp3",
                "BGM_retrieval/music_foder/9365387194-no_vocals-16.4.mp3",
                "BGM_retrieval/music_foder/6240562188-no_vocals-1.0.mp3",
                "BGM_retrieval/music_foder/14707685787-no_vocals-1.4.mp3"
            ]。
        bgm_version_control_th (int): bgm匹配服务版本切换比例，默认为50。

    InputTables:
        shot_table: 视频列表所在的表。
        request_table: 请求表。

    OutputTables:
        shot_table: 添加了BGM Blob Key 的表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/bgm_match_op/bgm_match_op_v2.py?ref_type=heads

    Examples：
        #待作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        video_list_column = self.attrs.get("video_list_column", "video_list")
        forced_bgm_blob_key = self.attrs.get("forced_bgm_blob_key", None)
        bgm_blob_key_column = self.attrs.get("bgm_blob_key_column", "bgm_blob_key")
        bgm_blob_key_list_column = self.attrs.get("bgm_blob_key_list_column", "bgm_blob_key_list")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        bgm_version_column = self.attrs.get("bgm_version_column", "bgm_version")
        random_bgm_list = self.attrs.get("random_bgm_list", [
            "BGM_retrieval/music_foder/3527242720-no_vocals-47.9.mp3",
            "BGM_retrieval/music_foder/3524459843-no_vocals-103.7.mp3",
            "BGM_retrieval/music_foder/9365387194-no_vocals-16.4.mp3",
            "BGM_retrieval/music_foder/6240562188-no_vocals-1.0.mp3",
            "BGM_retrieval/music_foder/14707685787-no_vocals-1.4.mp3"
        ])
        bgm_version_control_th = self.attrs.get("bgm_version_control_th", 50)

        # 强制设置特定的 bgm
        if forced_bgm_blob_key is not None:
            shot_table[bgm_blob_key_column] = [forced_bgm_blob_key] * len(shot_table)
            shot_table[bgm_version_column] = [0] * len(shot_table)
            op_context.output_tables.append(shot_table)
            return True

        second_industry_name = request_table[second_industry_name_column][0]
        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        bgm_retrieval_cfg: dict = kconf_params["bgm_retrieval_cfg"]
        no_vocal = bgm_retrieval_cfg["no_vocal"]
        # V2实验控制
        if random.randint(1, 100) <= bgm_version_control_th:
            bgm_match_version = 'V2'
        else:
            bgm_match_version = 'V1'

        bgm_retrieval_client = ClientManager().get_client_by_name("BgmRetrievalClient")
        shot_table[bgm_blob_key_column] = None
        shot_table[bgm_blob_key_list_column] = None
        shot_table[bgm_version_column] = None
        for index, row in shot_table.iterrows():
            video_list = row.get(video_list_column)
            if isinstance(video_list, tuple):
                video_list = [video_list]
            music_ids = bgm_retrieval_client.sync_req(video_list, no_vocal, bgm_match_version, second_industry_name)

            if music_ids:
                music_id = random.choice(music_ids)
                music_blob_key = build_bbs_resource_id(["ad", "nieuwland-material", music_id])
            else:
                bgm_match_version = 'V1'
                music_id = random.choice(random_bgm_list)
                music_blob_key = build_bbs_resource_id(["ad", "nieuwland-material", music_id])
            shot_table.at[index, bgm_blob_key_column] = music_blob_key
            shot_table.at[index, bgm_version_column] = '2' if bgm_match_version == 'V2' else '1'
            if music_ids:
                shot_table.at[index, bgm_blob_key_list_column] = [
                    build_bbs_resource_id(["ad", "nieuwland-material", music_id]) for music_id in music_ids
                ]

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(BgmMatchOpV2) \
    .add_input(name="shot_table", type="DataTable", desc="placeholder") \
    .add_input(name="request_table", type="DataTable", desc="placeholder") \
    .add_output(name="shot_table", type="DataTable", desc="placeholder") \
    .add_attr(name="video_list_column", type="str", desc="视频列表列名") \
    .add_attr(name="forced_bgm_blob_key", type="str", desc="强制指定bgm的 Blob Key") \
    .add_attr(name="bgm_blob_key_column", type="str", desc="音乐blob key结果列名") \
    .add_attr(name="bgm_blob_key_list_column", type="str", desc="bgm Blob Key 列表列名") \
    .add_attr(name="second_industry_name_column", type="str", desc="二级行业列名") \
    .add_attr(name="bgm_version_column", type="str",desc="bgm版本列名") \
    .add_attr(name="random_bgm_list", type="list", desc="随机bgm列表") \
    .add_attr(name="bgm_version_control_th", type="int", desc="bgm匹配服务版本切换比例")