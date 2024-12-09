from video_graph import logger
from video_graph.common.client.client_manager import ClientManager
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VtuberVideoMixOp(Op):
    """
    Function:
        虚拟人视频成片算子，实现从混剪视频、虚拟人视频到最终视频的逻辑

    TODO: 算子可拆分为多个子算子

    Attributes:
        shot_type_column (str): 镜头类型列名，默认为"shot_type"。
        montage_video_blob_key_column (str): 混剪视频 Blob Key 列名，默认为"montage_video_blob_key"。
        vtuber_video_bbox_column (str): 虚拟人视频 bbox 列名，默认为"vtuber_video_bbox"。
        vtuber_face_bbox_column (str): 虚拟人脸部 bbox 列名，默认为"vtuber_face_bbox"。
        vtuber_desk_bbox_column (str): 虚拟人桌面 bbox 列名，默认为"vtuber_desk_bbox"。
        vtuber_video_blob_key_column (str): 虚拟人视频 Blob Key 列名，默认为"vtuber_video_blob_key"。
        vtuber_name_column (str): 虚拟人名称列名，默认为"vtuber_name"。
        resolution_column (str): 分辨率列名，默认为"resolution"。
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        reco_text_column (str): 推荐文案列名，默认为"reco_text"。
        final_template_id_column (str): 最终模版 ID 列名，默认为"final_template_id"。
        template_id_column (str): 模版 ID 列名，默认为"template_id"。
        template_res_column (str): 模版分辨率列名，默认为"template_res"。
        vtuber_template_id_column (str): 虚拟人模版 ID 列名，默认为"vtuber_template_id"。
        vtuber_template_res_column (str): 虚拟人模版分辨率列名，默认为"vtuber_template_res"。
        input_version (str): 输入版本，默认为None。
        tts_duration_column (str): TTS 时长列名，默认为"tts_duration"。
        tts_caption_column (str): TTS 标题列名，默认为"tts_caption"。
        template_video_resource_column (str): 模版视频资源列名，默认为"template_video_resource"。
        template_video_result_column (str): 模版视频结果列名，默认为"template_video_result"。

    InputTables:
        shot_table: 镜头表格。
        timeline_table: 时间线表格。
        vtuber_shot_table: 虚拟人镜头表格。
        request_table: 请求表格。

    OutputTables:
        request_table: 添加了模版视频资源和模版视频结果的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/vtuber_human_op/vtuber_video_mix_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        timeline_table: DataTable = op_context.input_tables[1]
        vtuber_shot_table: DataTable = op_context.input_tables[2]
        request_table: DataTable = op_context.input_tables[3]
        shot_type_column = self.attrs.get("shot_type_column", "shot_type")
        montage_video_blob_key_column = self.attrs.get("montage_video_blob_key_column", "montage_video_blob_key")
        vtuber_video_bbox_column = self.attrs.get("vtuber_video_bbox_column", "vtuber_video_bbox")
        vtuber_face_bbox_column = self.attrs.get("vtuber_face_bbox_column", "vtuber_face_bbox")
        vtuber_desk_bbox_column = self.attrs.get("vtuber_desk_bbox_column", "vtuber_desk_bbox")
        vtuber_target_vhuman_bbox = self.attrs.get("vtuber_target_vhuman_bbox", None)
        vtuber_video_blob_key_column = self.attrs.get("vtuber_video_blob_key_column", "vtuber_video_blob_key")
        vtuber_name_column = self.attrs.get("vtuber_name_column", "vtuber_name")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        reco_text_column = self.attrs.get("reco_text_column", "reco_text")
        final_template_id_column = self.attrs.get("final_template_id_column", "final_template_id")
        template_id_column = self.attrs.get("template_id_column", "template_id")
        template_res_column = self.attrs.get("template_res_column", "template_res")
        vtuber_template_id_column = self.attrs.get("vtuber_template_id_column", "vtuber_template_id")
        vtuber_template_res_column = self.attrs.get("vtuber_template_res_column", "vtuber_template_res")
        vtuber_replace_background_column = self.attrs.get("vtuber_replace_background_column",
                                                          "vtuber_replace_background")
        input_version = self.attrs.get("input_version", None)
        tts_duration_column = self.attrs.get("tts_duration_column", "tts_duration")
        tts_caption_column = self.attrs.get("tts_caption_column", "tts_caption")
        template_video_resource_column = self.attrs.get("template_video_resource_column", "template_video_resource")
        template_video_result_column = self.attrs.get("template_video_result_column", "template_video_result")

        job_id = op_context.request_id
        resolution = request_table.at[0, resolution_column]
        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        reco_text = request_table.at[0, reco_text_column]
        if template_id_column in request_table:
            template_id = int(request_table.at[0, template_id_column])
            template_res = request_table.at[0, template_res_column]
        else:
            template_id = None
            template_res = None
        final_template_id = int(request_table.at[0, final_template_id_column])

        # 混剪视频
        montage_video_resources = []
        for index, row in timeline_table.iterrows():
            montage_video_blob_key = row.get(montage_video_blob_key_column)
            if montage_video_blob_key:
                montage_video_resources.append(montage_video_blob_key)
        # 数字人视频
        vtuber_video_info = {}
        for index, row in vtuber_shot_table.iterrows():
            vtuber_name = row.get(vtuber_name_column)
            vtuber_video_blob_key = row.get(vtuber_video_blob_key_column)
            vtuber_id_for_ytech = f"replace_vhuman_video_{index}"
            vtuber_background_id_for_ytech = f"replace_vhuman_background_{index}"
            if vtuber_replace_background_column in row:
                vtuber_replace_background = row.get(vtuber_replace_background_column)
                vtuber_video_info[vtuber_name] = [vtuber_video_blob_key, vtuber_id_for_ytech,
                                                  vtuber_replace_background, vtuber_background_id_for_ytech]
            else:
                vtuber_video_info[vtuber_name] = [vtuber_video_blob_key, vtuber_id_for_ytech, '', '']
        # 处理逻辑，得到时间线
        template_name_dict = {}
        template_name_cnt = 0
        vhuman_script_tts_info = []
        shot_start_time = 0
        for idx in range(len(shot_table)):
            # 添加镜头模版及是否展现数字人
            if input_version in ["dsp_virtual_human_v3-2", 'dsp_virtual_human_v3-3', 'dsp_virtual_human_v3-4',
                                 "dsp_virtual_human_v3-6", "dsp_virtual_human_v3-7"]:
                if template_id in template_name_dict:
                    shot_template_name = template_name_dict[template_id][0]
                else:
                    shot_template_name = f"vhuman_template_{template_name_cnt}"
                    template_name_dict[template_id] = [shot_template_name, template_res]
                    template_name_cnt += 1
                cur_shot_time_line = {
                    "tts_info": {},
                    "vhuman_info": [],
                    "template_name": shot_template_name,
                    "show_vhuman_video": True
                }
            else:
                cur_shot_time_line = {
                    "tts_info": {},
                    "vhuman_info": [],
                    "show_vhuman_video": True if shot_table.at[idx, shot_type_column] == 'vtuber' else False
                }
            # 添加镜头时间信息
            shot_content = ''
            for content in shot_table.at[idx, tts_caption_column]:
                shot_content += content[0] + '.'
            if shot_content.endswith('.'):
                shot_content = shot_content[:-1]
            shot_end_time = shot_start_time + float(shot_table.at[idx, tts_duration_column])
            cur_shot_time_line["tts_info"]["tts_content"] = shot_content
            cur_shot_time_line["tts_info"]["duration"] = float(shot_table.at[idx, tts_duration_column])
            cur_shot_time_line["tts_info"]["timestamp"] = [shot_start_time, shot_end_time]
            shot_start_time = shot_end_time
            if input_version in ["dsp_virtual_human_v3-1", "dsp_virtual_human_v3-2", 'dsp_virtual_human_v3-3',
                                 'dsp_virtual_human_v3-4', "dsp_virtual_human_v3-6", "dsp_virtual_human_v3-7"]:
                for vtuber_idx in range(len(vtuber_shot_table)):
                    tmp_vtuber_info = {}
                    vhuman_id = int(
                        vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][1].split('_')[-1])
                    tmp_vtuber_info["vhuman_id"] = vhuman_id
                    vhuman_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_video_bbox_column]
                    tmp_vtuber_info["vhuman_bbox"] = vhuman_bbox
                    if vtuber_face_bbox_column in vtuber_shot_table:
                        vhuman_face_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_face_bbox_column]
                        tmp_vtuber_info["vhuman_face_bbox"] = vhuman_face_bbox

                    if vtuber_desk_bbox_column in vtuber_shot_table:
                        desk_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_desk_bbox_column]
                        tmp_vtuber_info["desk_bbox"] = desk_bbox

                    if vtuber_target_vhuman_bbox:
                        tmp_vtuber_info["target_vhuman_bbox"] = vtuber_target_vhuman_bbox

                    vtuber_template_id = vtuber_shot_table.at[vtuber_idx, vtuber_template_id_column]
                    vtuber_template_res = vtuber_shot_table.at[vtuber_idx, vtuber_template_res_column]
                    if vtuber_template_id in template_name_dict:
                        vtuber_template_name = template_name_dict[vtuber_template_id][0]
                    else:
                        vtuber_template_name = f"vhuman_template_{template_name_cnt}"
                        template_name_dict[vtuber_template_id] = [vtuber_template_name, vtuber_template_res]
                        template_name_cnt += 1
                    tmp_vtuber_info["template_name"] = vtuber_template_name
                    if vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][3] != '':
                        tmp_vtuber_info["vhuman_background"] = \
                            vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][3]
                    cur_shot_time_line["vhuman_info"].append(tmp_vtuber_info)
            else:
                need_vtuber_name = shot_table.at[idx, vtuber_name_column]
                for vtuber_idx in range(len(vtuber_shot_table)):
                    if vtuber_shot_table.at[vtuber_idx, vtuber_name_column] != need_vtuber_name:
                        continue
                    tmp_vtuber_info = {}
                    vhuman_id = int(
                        vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][1].split('_')[-1])
                    tmp_vtuber_info["vhuman_id"] = vhuman_id
                    vhuman_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_video_bbox_column]
                    tmp_vtuber_info["vhuman_bbox"] = vhuman_bbox
                    if vtuber_face_bbox_column in vtuber_shot_table:
                        vhuman_face_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_face_bbox_column]
                        tmp_vtuber_info["vhuman_face_bbox"] = vhuman_face_bbox

                    if vtuber_desk_bbox_column in vtuber_shot_table:
                        desk_bbox = vtuber_shot_table.at[vtuber_idx, vtuber_desk_bbox_column]
                        tmp_vtuber_info["desk_bbox"] = desk_bbox

                    if vtuber_target_vhuman_bbox:
                        tmp_vtuber_info["target_vhuman_bbox"] = vtuber_target_vhuman_bbox

                    vtuber_template_id = vtuber_shot_table.at[vtuber_idx, vtuber_template_id_column]
                    vtuber_template_res = vtuber_shot_table.at[vtuber_idx, vtuber_template_res_column]
                    if vtuber_template_id in template_name_dict:
                        vtuber_template_name = template_name_dict[vtuber_template_id][0]
                    else:
                        vtuber_template_name = f"vhuman_template_{template_name_cnt}"
                        template_name_dict[vtuber_template_id] = [vtuber_template_name, vtuber_template_res]
                        template_name_cnt += 1
                    tmp_vtuber_info["template_name"] = vtuber_template_name
                    if vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][3] != '':
                        tmp_vtuber_info["vhuman_background"] = \
                            vtuber_video_info[vtuber_shot_table.at[vtuber_idx, vtuber_name_column]][3]
                    cur_shot_time_line["vhuman_info"].append(tmp_vtuber_info)

            vhuman_script_tts_info.append(cur_shot_time_line)

        # 请求模版服务
        template_video_client = ClientManager().get_client_by_name("TemplateVideoClient")
        resp = template_video_client.sync_req(job_id, montage_video_resources, vtuber_video_info,
                                              vhuman_script_tts_info,
                                              template_name_dict, first_industry_name, second_industry_name,
                                              final_template_id,
                                              reco_text, resolution[0], resolution[1])
        if resp is None or resp.get("resp_type") != 1:
            self.fail_reason = f"模版视频渲染失败: {resp.get('err_msg') if resp else 'timeout'}"
            self.trace_log.update({"fail_reason": self.fail_reason})
            logger.info(f"template video resp is error, resp:{resp}")
            return False

        request_table[template_video_resource_column] = None
        request_table[template_video_result_column] = None
        request_table.at[0, template_video_result_column] = resp
        request_table.at[0, template_video_resource_column] = resp.get("output_video_resource", "")

        op_context.output_tables.append(request_table)
        return True


op_register.register_op(VtuberVideoMixOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="montage_resp_table", type="DataTable", desc="混剪视频结果表") \
    .add_input(name="vtuber_resp_table", type="DataTable", desc="虚拟人结果表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="request_table", type="DataTable", desc="请求表") \
    .add_attr(name="vtuber_target_vhuman_bbox", type="list", desc="虚拟人位置bbox")
