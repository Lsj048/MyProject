import os.path
import random
import uuid

from video_graph.common.client.azure_tts_client import text2audio as azure_tts
from video_graph.common.client.minimax_tts_client import text2audio as minimax_tts
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.logger import logger
from video_graph.data_table import DataTable
from video_graph.op import Op,op_register
from video_graph.op_context import OpContext


class TextToAudioOp(Op):
    """
    【remote】文字转音频TTS算子，支持Azure和Minimax两种TTS引擎，根据reading_track自动选择不同的引擎，输出音频文件、音频时长和音频字幕

    Attributes:
        text_column (str): 文本列名，默认为"text"
        tts_column (str): TTS音频列名，默认为"tts"
        tts_duration_column (str): TTS音频时长列名，默认为"tts_duration"
        tts_caption_column (str): TTS音频字幕列名，默认为"tts_caption"
        azure_proxy_hostname (str): Azure代理主机名，默认为"10.66.69.27"
        azure_proxy_port (int): Azure代理端口，默认为11080
        speed (float): 语速，默认为1.0
        forced_reading_track (str): 强制设置的朗读轨道列名，默认为None
        reading_track_column (str): 朗读轨道列名，默认为"reading_track"
        random_reading_track_list (list): 随机朗读轨道列表，默认为["混剪女声"]

    InputTables:
        shot_table: 文本所在的表

    OutputTables:
        shot_table: 添加了TTS音频信息的表

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/audio_process_op/text_to_audio_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.audio_process_op.text_to_audio_op import *

        # 创建一个包含text文本信息的模拟数据表
        input_table = DataTable(
            name = "TestTable",
            data = {
                'text_need_to_audio':['谁是这个世界上最帅的男人？不用想，我觉得就是你']
            }
        )
        # 设置 OpContext
        #process_id是用于设置存放输出文件的文件夹，request_id和thread_id用于组成文件名前缀，可对比输出表的文件路径查看
        op_context = OpContext("graph_name", "request_tag", "request_id")
        op_context.process_id = "test_op"
        op_context.request_id = "test-to"
        op_context.thread_id = thread_id = "audio"
        op_context.input_tables.append(input_table)

        # 实例化 TextToAudioOp 并运行，，算子通过self.attrs.get得到输入表中对应处理的列名，所以attrs中的命名方式需要与输入表data中的相统一
        text_to_audio_op = TextToAudioOp(
            name='TextToAudioOp',
            attrs={
            "text_column": "text_need_to_audio",
        })
        success = text_to_audio_op.process(op_context)

        # 检查结果
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子计算失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        text_column = self.attrs.get("text_column", "text")
        tts_column = self.attrs.get("tts_column", "tts")
        tts_duration_column = self.attrs.get("tts_duration_column", f"{tts_column}_duration")
        tts_caption_column = self.attrs.get("tts_caption_column", f"{tts_column}_caption")
        azure_proxy_hostname = self.attrs.get("azure_proxy_hostname", "10.66.69.27")
        azure_proxy_port = self.attrs.get("azure_proxy_port", 11080)
        speed = self.attrs.get("speed", 1.0)
        # 音色设置，forced_reading_track > reading_track_column > random_reading_track_list
        forced_reading_track = self.attrs.get("forced_reading_track", None)
        reading_track_column = self.attrs.get("reading_track_column", "reading_track")
        random_reading_track_list = self.attrs.get("random_reading_track_list", ["混剪女声", "和蔼男声120"])
        tts_filename_prefix = f"{op_context.request_id}-{op_context.thread_id}"
        file_directory = f"{op_context.process_id}"

        kconf_params: dict = get_kconf_value("ad.algorithm.nieuwlandGeneration", "json")
        tts_cfg: dict = kconf_params["tts_cfg"]

        shot_table[tts_column] = None
        shot_table[tts_duration_column] = 0.0
        shot_table[tts_caption_column] = None
        for index, row in shot_table.iterrows():
            audio_file_name = f"{tts_filename_prefix}-{str(uuid.uuid1())}.wav"
            audio_file_path = os.path.join(file_directory, audio_file_name)
            texts = row.get(text_column)
            if not texts or sum([len(text) for text in texts]) == 0:
                continue

            reading_track = forced_reading_track
            if reading_track is None:
                random_reading_track = random.choice(random_reading_track_list)
                reading_track = row.get(reading_track_column, random_reading_track)
            if reading_track in tts_cfg["style_new"]:
                speaker_id, speaking_style, tts_server_idx, tts_speed = tts_cfg["style_new"][reading_track]
            else:
                speaker_id, speaking_style, tts_server_idx, tts_speed = tts_cfg["style_new"]["default"]

            if tts_server_idx == 3:
                duration, caption = minimax_tts(texts, voice_id=speaker_id, output=audio_file_path,
                                                req_id=op_context.request_id, speed=tts_speed * speed)
            elif tts_server_idx == 2:
                duration, caption = azure_tts(texts, voice_id=speaker_id, output=audio_file_path,
                                              req_id=op_context.request_id, speed=tts_speed * speed,
                                              proxy_hostname=azure_proxy_hostname, proxy_port=azure_proxy_port,
                                              speaking_style=speaking_style)
            else:
                logger.error(f"tts_server_idx:{tts_server_idx} is not in [2, 3]")
                op_context.perf_ctx("tts_server_idx_error", extra1=str(tts_server_idx), extra2=speaker_id,
                                    extra3=op_context.graph_name)
                duration = 0
                caption = []

            if duration == 0 or len(caption) == 0:
                self.fail_reason = f'文字转音频失败，req_server：{"azure_tts" if tts_server_idx == 2 else "minimax_tts"}'
                self.trace_log.update({"fail_reason": self.fail_reason})
                op_context.perf_ctx("text_to_audio_failed", extra1=str(tts_server_idx))
                return False

            shot_table.at[index, tts_column] = audio_file_path
            shot_table.at[index, tts_duration_column] = duration
            shot_table.at[index, tts_caption_column] = caption

        op_context.output_tables.append(shot_table)
        return True


op_register.register_op(TextToAudioOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_output(name="shot_table", type="DataTable", desc="镜号表") \
    .add_attr(name="text_column", type="str", desc="文本列名") \
    .add_attr(name="tts_column", type="str", desc="tts列名") \
    .add_attr(name="tts_duration_column", type="str", desc="TTS音频时长列名") \
    .add_attr(name="tts_caption_column", type="str", desc="TTS音频字幕列名") \
    .add_attr(name="azure_proxy_hostname", type="str", desc="Azure代理主机名") \
    .add_attr(name="azure_proxy_port", type="int", desc="Azure代理端口") \
    .add_attr(name="speed", type="float", desc="语速") \
    .add_attr(name="forced_reading_track", type="str", desc="强制设置的朗读轨道列名") \
    .add_attr(name="reading_track_column", type="str", desc="音色配置") \
    .add_attr(name="random_reading_track_list", type="list", desc="随机朗读轨道列表") \
    .set_parallel(True)
