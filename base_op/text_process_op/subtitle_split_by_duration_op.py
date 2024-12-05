import json
import math
import os

from video_graph import logger
from video_graph.common.utils.tools import parse_bbs_resource_id
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class SubtitleSplitByDurationOp(Op):
    """
    【local】将视频的字幕信息按照切片时长分割

    Attributes:
        input:
        video_blob_key_column (str, optional):  视频 BlobKey 所在的列名，默认为 'video_blob_key'
        video_duration_column (str, optional): 视频时长列名，默认为 'duration'
        video_segment_duration (int, optional): 每切片时长 (单位为秒), 默认为 10s
        video_subtitle_column (str, optional): 完整视频的字幕检测结果所在列, 默认为 'video_subtitle'
        save_segment_bbox_as_json (bool, optional): 是否要把视频片段的 bbox 结果以 json 形式存储到本地, 默认 False

        output:
        video_segment_bbox_column (str, optional): 视频切片的对应 bbox 信息所在列, 默认为 'video_segment_bbox'
        video_segment_index_column (str, optional): 视频切片索引列名，默认为 'video_segment_index'
        video_segment_bbox_json_path_column (str, optional): 视频切片对应的 bbox 信息本地文件路径,
                                                             默认为 'video_segment_bbox_json_path'
        video_segment_bbox_json_blob_key_column (str, optional): 视频切片对应的 bbox 信息上传 BlobKey,
                                                                 默认为 'video_segment_bbox_json_blob_key'

    InputTables:
        material_table: 完整视频对应的字幕所在表格

    OutputTables:
        ocr_segment_table: 根据时长切分后的视频片段对应字幕所在表格

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/text_process_op/subtitle_split_by_duration_op.py?ref_type=heads

    Examples:
        from video_graph.ops.base_op.text_process_op.subtitle_split_by_duration_op import *

        # 创建输入表格
        input_table = DataTable(
            name="TestTable1",
            data = {
                'video_blob_key': ['video_blob_key_1'],
                'duration': [120],  # 视频时长，单位秒
                'video_subtitle': [
                    [
                        {'bbox': [10, 20, 30, 40], 'start_time': 10.0, 'end_time': 20.0, 'textType': 'text', 'score': 0.9},
                        {'bbox': [50, 60, 70, 80], 'start_time': 30.0, 'end_time': 40.0, 'textType': 'text', 'score': 0.8}
                    ]
                ]
            }
        )

        # 创建操作上下文
        op_context = OpContext(graph_name="test_graph", request_tag="test_tag", request_id="12345")
        op_context.input_tables.append(input_table)

        # 配置并实例化算子
        subtitle_split_by_duration_op = SubtitleSplitByDurationOp(
            name="SubtitleSplitByDurationOp",
            attrs={
                'video_blob_key_column': 'video_blob_key',
                'video_duration_column': 'duration',
                'video_segment_duration': 20,
                'video_subtitle_column': 'video_subtitle',
                'save_segment_bbox_as_json': True
            }
        )

        # 执行算子
        success = subtitle_split_by_duration_op.process(op_context)

        # 检查输出表格
        if success:
            output_table = op_context.output_tables[0]
            display(output_table)
        else:
            print("算子执行失败")
    """

    def compute(self, op_context: OpContext) -> bool:
        material_table: DataTable = op_context.input_tables[0]
        video_blob_key_column = self.attrs.get('video_blob_key_column', 'video_blob_key')
        video_duration_column = self.attrs.get('video_duration_column', 'duration')
        video_segment_duration = self.attrs.get('video_segment_duration', 10)
        video_subtitle_column = self.attrs.get('video_subtitle_column', 'video_subtitle')
        save_segment_bbox_as_json = self.attrs.get('save_segment_bbox_as_json', False)

        all_bbox_info = []
        video_segment_index_list = []
        video_segment_bbox_json_path_list = []
        video_segment_bbox_json_blob_key_list = []

        video_index = 0
        for index, row in material_table.iterrows():
            video_index += 1
            video_duration = row.get(video_duration_column)
            segment_num = math.ceil(video_duration / video_segment_duration)
            segments_bbox = [[] for _ in range(segment_num)]

            video_subtitle = row.get(video_subtitle_column)
            if not isinstance(video_subtitle, list) or len(video_subtitle) == 0:
                logger.error('unexpected video subtitle info')
                continue

            for item in video_subtitle:
                bbox = item['bbox']
                start_time = float(item['start_time'])
                end_time = float(item['end_time'])
                text_type = item['textType']
                score = item['score']

                start_segment = math.floor(start_time / video_segment_duration)
                end_segment = math.ceil(end_time / video_segment_duration)

                for i in range(start_segment, end_segment):
                    segment_start_time = i * video_segment_duration
                    segment_end_time = (i + 1) * video_segment_duration
                    segment_bbox = {'start_time': max(segment_start_time, round(start_time, 1)) - segment_start_time,
                                    'end_time': min(segment_end_time, round(end_time, 1)) - segment_start_time,
                                    'bbox': [int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])],
                                    'textType': text_type,
                                    'score': score}
                    segments_bbox[i].append(segment_bbox)

            all_bbox_info.extend(segments_bbox)
            video_segment_index_list.extend([f'{video_index}_{segment_idx}'
                                             for segment_idx in range(segment_num)])

            if save_segment_bbox_as_json:
                bbox_root = os.path.join(op_context.process_id)
                if not os.path.exists(bbox_root):
                    os.makedirs(bbox_root)
                video_blob_key = row.get(video_blob_key_column)
                _, _, video_name = parse_bbs_resource_id(video_blob_key)
                for i in range(segment_num):
                    if len(segments_bbox[i]) == 0:
                        bbox_file_path = None
                        bbox_file_name = None
                    else:
                        bbox_file_name = video_name.replace(
                            '.mp4', f'-segment-bbox-{i:04}-{op_context.request_id}-{op_context.thread_id}.json')
                        bbox_file_path = os.path.join(bbox_root, bbox_file_name)
                        with open(bbox_file_path, 'w') as f:
                            json.dump(segments_bbox[i], f)
                    video_segment_bbox_json_path_list.append(bbox_file_path)
                    video_segment_bbox_json_blob_key_list.append(bbox_file_name)

        video_segment_bbox_column = self.attrs.get('video_segment_bbox_column', 'video_segment_bbox')
        video_segment_index_column = self.attrs.get('video_segment_index_column', 'video_segment_index')
        video_segment_bbox_json_path_column = self.attrs.get('video_segment_bbox_json_path_column',
                                                             'video_segment_bbox_json_path')
        video_segment_bbox_json_blob_key_column = self.attrs.get('video_segment_bbox_json_blob_key_column',
                                                                 'video_segment_bbox_json_blob_key')
        output_table_dict = {
            video_segment_bbox_column: all_bbox_info,
            video_segment_index_column: video_segment_index_list
        }
        if save_segment_bbox_as_json:
            output_table_dict[video_segment_bbox_json_path_column] = video_segment_bbox_json_path_list
            output_table_dict[video_segment_bbox_json_blob_key_column] = video_segment_bbox_json_blob_key_list

        ocr_segment_table = DataTable(name='ocr_segment_table', data=output_table_dict)
        op_context.output_tables.append(ocr_segment_table)
        return True


op_register.register_op(SubtitleSplitByDurationOp) \
    .add_input(name="material_table", type="DataTable", desc="完整视频对应的字幕所在表格") \
    .add_output(name="ocr_segment_table", type="DataTable", desc="根据时长切分后的视频片段对应字幕所在表格") \
    .add_attr(name="video_blob_key_column", type="str", desc="视频 BlobKey 所在的列名") \
    .add_attr(name="video_duration_column", type="str", desc="视频时长列名") \
    .add_attr(name="video_segment_duration", type="int", desc="每切片时长 (单位为秒)") \
    .add_attr(name="video_subtitle_column", type="str", desc="完整视频的字幕检测结果所在列") \
    .add_attr(name="save_segment_bbox_as_json", type="bool", desc="是否要把视频片段的 bbox 结果以 json 形式存储到本地") \
    .add_attr(name="video_segment_bbox_column", type="str", desc="视频切片的对应 bbox 信息所在列") \
    .add_attr(name="video_segment_index_column", type="str", desc="视频切片索引列名") \
    .add_attr(name="video_segment_bbox_json_path_column", type="str", desc="视频切片对应的 bbox 信息本地文件路径") \
    .add_attr(name="video_segment_bbox_json_blob_key_column", type="str", desc="视频切片对应的 bbox 信息上传 BlobKey")
