import random

from video_graph.common.client.text_video_match_client import text_video_match
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TextVideoMatchOp(Op):
    """
    Function:
        文本视频匹配算子，输出匹配的视频列表和原始的匹配结果

    Attributes:
        fixed_caption_column (str): 固定字幕列名，默认为"fixed_caption"
        tts_caption_column (str): TTS字幕列名，默认为"tts_caption"
        tts_duration_column (str): TTS时长列名，默认为"tts_duration"
        clip_resource_info_column (str): 视频资源信息列名，默认为"clip_resource_info"
        video_match_res_column (str): 视频匹配结果列名，默认为"video_match_res"
        video_list_column (str): 视频列表列名，默认为"video_list"
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"
        shot_type_column (str): 镜号类型列名，默认为"shot_type"
        render_video_list_column (str): 渲染用的视频列表信息列名，默认为"render_video_list"
        random_match_cover (bool): 是否随机匹配兜底，默认为False

    InputTables:
        request_table: 请求表
        shot_table: 镜号表

    OutputTables:
        shot_table: 添加了视频匹配结果的镜号表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_match_op/text_video_match_op.py?ref_type=heads

    Examples:
        #等待作者更新～
    """

    def random_match_strategy(self, shot_table: DataTable, tts_duration_column: str,
                              clip_resource_info: list, op_context: OpContext,
                              video_list_column: str, render_video_list_column: str,
                              text_video_match_type_column: str) -> bool:
        for index, row in shot_table.iterrows():
            tts_duration = row.get(tts_duration_column)

            # 随机打散
            random.shuffle(clip_resource_info)

            # 音频-视频时间对齐
            video_list = []
            render_video_list_list = []
            remain_duration = tts_duration
            for info in clip_resource_info:
                video_index, video_blob_key, start_time, end_time = info[0:4]
                start_time = start_time / 1000.0
                end_time = end_time / 1000.0
                duration = end_time - start_time
                if duration > remain_duration:
                    display_start_time = start_time
                    display_end_time = start_time + remain_duration
                    remain_duration = 0
                    video_list.append((video_blob_key, 0, display_end_time - display_start_time))
                    render_video_list_list.append((int(video_index) - 1, display_start_time, display_end_time, 1.0))
                    break
                else:
                    display_start_time = start_time
                    display_end_time = end_time
                    remain_duration -= duration
                    video_list.append((video_blob_key, 0, display_end_time - display_start_time))
                    render_video_list_list.append((int(video_index) - 1, display_start_time, display_end_time, 1.0))

            if remain_duration > 0:
                self.fail_reason = (f"视频时长太短，文本时长：{tts_duration}，视频个数：{len(clip_resource_info)}，"
                                    f"时长差距：{remain_duration}")
                self.trace_log.update({"fail_reason": self.fail_reason})
                return False

            shot_table.at[index, video_list_column] = video_list
            shot_table.at[index, render_video_list_column] = render_video_list_list
            shot_table[text_video_match_type_column] = "random"
        op_context.output_tables.append(shot_table)
        return True

    def compute(self, op_context: OpContext) -> bool:
        request_table: DataTable = op_context.input_tables[0]
        shot_table: DataTable = op_context.input_tables[1]
        fixed_caption_column = self.attrs.get("fixed_caption_column", "fixed_caption")
        tts_caption_column = self.attrs.get("tts_caption_column", "tts_caption")
        tts_duration_column = self.attrs.get("tts_duration_column", "tts_duration")
        clip_resource_info_column = self.attrs.get("clip_resource_info_column", "clip_resource_info")
        video_match_res_column = self.attrs.get("video_match_res_column", "video_match_res")
        video_list_column = self.attrs.get("video_list_column", "video_list")
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        shot_type_column = self.attrs.get("shot_type_column", "shot_type")
        render_video_list_column = self.attrs.get("render_video_list_column", "render_video_list")
        random_match_cover = self.attrs.get("random_match_cover", False)
        text_video_match_type_column = self.attrs.get("text_video_match_type_column", 'text_video_match_type')

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        text_video_match_cfg = kconf_params["text_match_cfg"]
        need_asd = text_video_match_cfg.get("need_asd").get(op_context.request_tag, False)

        first_industry_name = request_table.loc[0, first_industry_name_column]
        second_industry_name = request_table.loc[0, second_industry_name_column]
        fixed_caption = request_table.loc[0, fixed_caption_column]

        shot_table[video_list_column] = None
        shot_table[video_match_res_column] = None
        shot_table[render_video_list_column] = None
        shot_table[text_video_match_type_column] = "model"
        for index, row in shot_table.iterrows():
            if "montage" not in row.get(shot_type_column):
                continue

            tts_caption = row.get(tts_caption_column)
            clip_resource_info = row.get(clip_resource_info_column)
            script_info = [(f"{text}，{fixed_caption}", start_time, end_time)
                           for text, start_time, end_time in tts_caption]

            res_info = text_video_match(op_context.request_id, script_info, clip_resource_info, need_asd=need_asd,
                                        first_industry_name=first_industry_name,
                                        second_industry_name=second_industry_name,
                                        source_type=op_context.request_tag)

            if res_info is None or res_info.get('isSuccess', False) is False:
                if random_match_cover:
                    return self.random_match_strategy(shot_table, tts_duration_column, clip_resource_info, op_context,
                                                      video_list_column, render_video_list_column,
                                                      text_video_match_type_column)

                self.fail_reason = f'台词视频匹配失败：{res_info.get("error_info") if res_info else "res_none"}'
                self.trace_log.update({"fail_reason": self.fail_reason})
                logger.error(f"request_id[{op_context.request_id}] text video match failed, message:{res_info}")
                op_context.perf_ctx("text_video_match_failed",
                                    extra1=str(res_info.get("result_code")) if res_info else "res_none",
                                    extra2=self.fail_reason)
                return False

            # video_list和render_video_list_list存储的都是匹配结果的切片list
            # 区别是：
            # * video_list是切片粒度的数据，用于bgm匹配
            # * render_video_list_list是原始视频粒度的数据，用于视频渲染
            video_list = []
            render_video_list_list = []
            for line_video in res_info.get("clips_info", []):
                tts_start_time = line_video.get("tts_start_time")
                tts_end_time = line_video.get("tts_end_time")
                tts_duration = tts_end_time - tts_start_time
                video_clips_duration = line_video.get("video_clips_duration")
                speed = video_clips_duration * 1.0 / tts_duration
                line_video_list = []
                for video_clip in line_video.get("video_clips", []):
                    resource_id = video_clip.get("resource_id")  # 切片资源id
                    video_idx = int(video_clip.get("video_idx"))  # 切片所在原始视频在表中的索引id
                    start_time = video_clip.get("video_start_time")  # 切片在原始视频中的开始时间
                    end_time = video_clip.get("video_end_time")  # 切片在原始视频中的结束时间
                    video_list.append((resource_id, 0, end_time - start_time))
                    line_video_list.append((video_idx - 1, start_time, end_time, speed))
                render_video_list_list.append(line_video_list)

            if len(video_list) == 0:
                if random_match_cover:
                    return self.random_match_strategy(shot_table, tts_duration_column, clip_resource_info, op_context,
                                                      video_list_column, render_video_list_column,
                                                      text_video_match_type_column)

                self.fail_reason = '台词视频匹配失败：匹配结果为空'
                self.trace_log.update({"fail_reason": self.fail_reason})
                logger.error(f"request_id[{op_context.request_id}] matched video_list empty, message:{res_info}")
                op_context.perf_ctx("matched_video_list_empty")
                return False

            shot_table.at[index, video_list_column] = video_list
            shot_table.at[index, video_match_res_column] = res_info
            shot_table.at[index, render_video_list_column] = render_video_list_list

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(TextVideoMatchOp) \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="fixed_caption_column", type="str", desc="固定caption列名") \
    .add_attr(name="tts_caption_column", type="str", desc="tts caption列名") \
    .add_attr(name="tts_duration_column", type="str", desc="tts时长列名") \
    .add_attr(name="clip_resource_info_column", type="str", desc="切镜资源信息列名") \
    .add_attr(name="video_match_res_column", type="str", desc="匹配结果列名") \
    .add_attr(name="video_list_column", type="str", desc="视频列表") \
    .add_attr(name="first_industry_name_column", type="str", desc="一级行业名称列名") \
    .add_attr(name="second_industry_name_column", type="str", desc="二级行业名称列名") \
    .add_attr(name="shot_type_column", type="str", desc="镜号类型列名") \
    .add_attr(name="render_video_list_column", type="str", desc="渲染视频列表信息列名") \
    .add_attr(name="random_match_cover", type="bool", desc="是否随机匹配兜底")
