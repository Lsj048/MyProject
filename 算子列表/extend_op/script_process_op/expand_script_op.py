# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# Author     ：Bo Wang
# File       : script_op.py
# Time       ：2024/11/4 17:50
"""
import json
import time
import traceback

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.logger import logger
from video_graph.common.utils.parse_script import encode_script, parse_script, merge_text, check_dsp_live_montage, split_subtitle
from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class ExpandScriptOp(Op):
    """
    Function:
        对当前脚本进行扩展

    Attributes:
        llm_input_column (str): llm输入数据字段，默认为"asr_texts"。

    InputTables:
        shot_table: 脚本表。
        material_table: 素材所在表

    OutputTables:
        shot_table: 改写后的脚本表。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/script_process_op/expand_script_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        material_table: DataTable = op_context.input_tables[2]

        llm_input_column = self.attrs.get("llm_input_column", "asr_texts")

        asr_list = []
        for index, row in material_table.iterrows():
            llm_input = row.get(llm_input_column)
            if str(llm_input) == 'nan' or llm_input is None:
                continue
            for _text in llm_input:
                asr_list.append(_text)
            if sum([len(_t) for _t in asr_list]) > 1000:
                break

        asr_len = sum([len(_t) for _t in asr_list])
        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        if asr_len < kconf_params["aigc2_server_cfg"].get("min_total_asr_len", 100):
            logger.error(f"request_id {request_table['request_id'][0]}: 口播片段素材过少")
            self.fail_reason = "口播片段素材过少"
            self.trace_log.update({"fail_reason": self.fail_reason})
            op_context.output_tables.append(material_table)
            return False
        pre_subtitles = shot_table["script"][0]
        pre_subtitles = "<SEP>".join(pre_subtitles)
        _retry_num = 0
        subtitles = None
        _start_time = time.perf_counter()
        while _retry_num < 5:
            try:
                photo_asr_client = ClientManager().get_client_by_name('LLMLiveScriptClient')
                subtitles = photo_asr_client.sync_req(
                    uuid=request_table["request_id"][0],
                    item_live_asr="<SEP>".join(asr_list),
                    item_live_author='', item_title='')
            except:
                logger.error(f"{traceback.format_exc()}")
                _retry_num += 1
                time.sleep(5)
                continue
            if subtitles:
                modify_script = parse_script(subtitles)
                modify_script = merge_text(modify_script, key="视频类型", value="台词",
                                           shot_types=['独白', '不在当前直播中的片段'])
                subtitles = encode_script(modify_script)
                tail_shot = f"|8|固定镜头|近景|无|{pre_subtitles}|行动号召|号召进入直播间、关注或者购买|无|独白|"
                subtitles = "\n".join(subtitles.split('\n')[:-1] + [tail_shot])
                if '直播片段' in subtitles and check_dsp_live_montage(subtitles):
                    break
            _retry_num += 1
            logger.info("retry external_live_montage")
            time.sleep(5)
        op_context.perf_ctx("nieuwland.generation.cost", extra1="external_live_montage",
                            micros=int((time.perf_counter() - _start_time) * 1000))

        if subtitles is None:
            self.fail_reason = "脚本服务调用失败"
            self.trace_log.update({"fail_reason": self.fail_reason})
            op_context.output_tables.append(material_table)
            return False
        if '直播片段' not in subtitles:
            self.fail_reason = "脚本未生成直播镜号"
            self.trace_log.update({"fail_reason": self.fail_reason})
            op_context.output_tables.append(material_table)
            return False
        if not check_dsp_live_montage(subtitles):
            self.fail_reason = "脚本台词出现<号召进入直播间>等话术"
            self.trace_log.update({"fail_reason": self.fail_reason})
            op_context.output_tables.append(material_table)
            return False

        shot_table["markdown_script"] = None
        shot_table["markdown_script"][0] = subtitles
        op_context.output_tables.append(shot_table)
        return True

op_register.register_op(ExpandScriptOp) \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_input(name="shot_table", type="DataTable", desc="脚本表") \
    .add_input(name="material_table", type="DataTable", desc="资源表") \
    .add_output(name="shot_table", type="DataTable", desc="脚本表") \
    .add_attr(name="llm_input_column", type="str", desc="asr_texts")
