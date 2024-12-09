from video_graph.common.utils.tools import LiveAlignment
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TextVideoAsrAlignmentOp(Op):
    """
    Function:
        文本和视频ASR对齐算子，输出ASR字幕和视频列表

    Attributes:
        asr_texts_column (str): ASR文本列名，默认为"asr_texts"
        asr_start_end_column (str): ASR开始结束时间列名，默认为"asr_start_end"
        subtitles_column (str): 字幕列名，默认为"台词"
        filter_duration_th (float): 过滤时长阈值，默认为1.5
        merge_th (float): 合并阈值，默认为0.5
        video_blob_key_column (str): 视频Blob Key列名，默认为"video_blob_key"
        video_duration_column (str): 视频时长列名，默认为"duration"
        video_list_column (str): 视频列表列名，默认为"video_list"
        asr_caption_column (str): ASR字幕列名，默认为"asr_caption"
        render_video_list_column (str): 渲染用的视频列表信息列名，默认为"render_video_list"

    InputTables:
        shot_table: 镜号表
        material_table: 素材表

    OutputTables:
        shot_table: 添加了ASR字幕和视频列表的镜号表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_match_op/text_video_asr_alignment_op.py?ref_type=heads

    Examples:
        #等待作者更新～
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        material_table: DataTable = op_context.input_tables[1]
        asr_texts_column = self.attrs.get("asr_texts_column", "asr_texts")
        asr_start_end_column = self.attrs.get("asr_start_end_column", "asr_start_end")
        subtitles_column = self.attrs.get("subtitles_column", "台词")
        filter_duration_th = self.attrs.get("filter_duration_th", 1.5)
        merge_th = self.attrs.get("merge_th", 0.5)
        video_blob_key_column = self.attrs.get("video_blob_key_column", "video_blob_key")
        video_duration_column = self.attrs.get("video_duration_column", "duration")
        video_list_column = self.attrs.get("video_list_column", "video_list")
        render_video_list_column = self.attrs.get("render_video_list_column", "render_video_list")
        asr_caption_column = self.attrs.get("asr_caption_column", "asr_caption")
        duration_column = self.attrs.get("duration_column", "duration")

        asr_texts_list = []
        asr_start_end_list = []
        asr_video_index_list = []
        for index, row in material_table.iterrows():
            asr_texts = row.get(asr_texts_column)
            asr_start_end = row.get(asr_start_end_column)
            if asr_texts is None or asr_start_end is None:
                continue
            asr_texts_list.extend(asr_texts)
            asr_start_end_list.extend(asr_start_end)
            asr_video_index_list.extend([index] * len(asr_texts))

        shot_table[video_list_column] = None
        shot_table[render_video_list_column] = None
        shot_table[asr_caption_column] = None
        shot_table[duration_column] = 0.0
        for index, row in shot_table.iterrows():
            subtitles = row.get(subtitles_column)
            alignment = LiveAlignment(asr_texts_list, asr_start_end_list, asr_video_index_list, subtitles)
            time_clip_list, subtitles_list = alignment(filter_duration_th=filter_duration_th, merge_th=merge_th)

            video_list = []
            render_video_list = []
            asr_caption = []
            total_duration = 0
            for time_clip_idx, (
            asr_start_idx, video_start_idx, start_time, asr_end_idx, video_end_start_idx, end_time) in enumerate(
                    time_clip_list):
                cur_duration = end_time - start_time
                if video_start_idx == video_end_start_idx:  # 只包含一个视频的情况
                    video_blob_key = material_table.at[video_start_idx, video_blob_key_column]
                    video_list.append((video_blob_key, start_time, end_time))
                    render_video_list.append((video_start_idx, start_time, end_time, 1.0))
                else:  # 包含多个视频的情况
                    for video_idx in range(video_start_idx, video_end_start_idx + 1):
                        duration = material_table.at[video_idx, video_duration_column]
                        video_blob_key = material_table.at[video_idx, video_blob_key_column]
                        if video_idx == video_start_idx:
                            cur_time_start = start_time
                            cur_time_end = duration
                        elif video_idx == video_end_start_idx:
                            cur_time_start = 0
                            cur_time_end = end_time
                        else:
                            cur_time_start = 0
                            cur_time_end = duration
                        video_list.append((video_blob_key, cur_time_start, cur_time_end))
                        render_video_list.append((video_idx, cur_time_start, cur_time_end, 1.0))

                cur_asr_duration = 0
                for asr_video_idx, (asr_start_time, asr_end_time), asr_text in subtitles_list[time_clip_idx]:
                    asr_caption.append((asr_text, total_duration + cur_asr_duration,
                                        total_duration + cur_asr_duration + asr_end_time - asr_start_time))
                    cur_asr_duration += asr_end_time - asr_start_time
                total_duration += cur_duration

            shot_table.at[index, duration_column] = total_duration
            shot_table.at[index, asr_caption_column] = asr_caption
            shot_table.at[index, video_list_column] = video_list
            shot_table.at[index, render_video_list_column] = render_video_list

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(TextVideoAsrAlignmentOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="material_table", type="DataTable", desc="素材表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="asr_texts_column", type="str", desc="asr文本列名") \
    .add_attr(name="asr_start_end_column", type="str", desc="asr开始结束时间列名") \
    .add_attr(name="subtitles_column", type="str", desc="字幕列名") \
    .add_attr(name="filter_duration_th", type="int", desc="过滤时长阈值") \
    .add_attr(name="merge_th", type="int", desc="合并阈值") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频blobstore地址列名") \
    .add_attr(name="video_duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="video_list_column", type="str", desc="视频列表列名") \
    .add_attr(name="asr_caption_column", type="str", desc="asr caption列名")
