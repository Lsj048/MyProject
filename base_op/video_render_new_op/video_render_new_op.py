import json

from google.protobuf.json_format import MessageToJson
from ks_editor_kernel.model_builder.video_project_model_builder import EditSceneVideoProjectBuilder

from video_graph.common.client.client_manager import ClientManager
from video_graph.common.proto.rpc_proto.project_composite_pb2 import ProjectCompositeInstruction, CompositeRenderType, \
    VideoCompositeParam, Rational, VideoCodecType, CompositeCodecParamPreset
from video_graph.common.utils.blobstore import BlobStoreClientManager
from video_graph.common.utils.logger import logger
from video_graph.common.utils.tools import parse_bbs_resource_id, get_timestamp
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class VideoRenderNewOp(Op):
    """
    【remote】视频渲染算子，根据项目模型构建器和输入文件映射，发送到Minecraft服务端渲染视频

    Attributes:
        project_model_builder_column (str): 项目模型构建器列名，默认为"project_model_builder"。
        input_file_map_column (str): 输入文件映射列名，默认为"input_file_map"。
        video_output_blob_key_column (str): 视频输出blob key列名，默认为"video_output_blob_key"。
        rpc_status_column (str): rpc状态列名，默认为"rpc_status"。
        rpc_message_column (str): rpc消息列名，默认为"rpc_message"。
        video_render_res_column (str): 视频渲染结果列名，默认为"video_render_res"。
        video_render_param_column (str): 视频渲染参数列名，默认为"video_render_param"。
        blob_db (str): blob数据库，默认为"ad"。
        blob_table (str): blob表格，默认为"nieuwland-material"。
        font_map_column (str): 字体映射列名，默认为"font_map"。
        render_biz_name (str): 渲染业务名，默认为"nieuwland"。
        project_model_blob_key_column (str): 项目模型blob key列名，默认为"project_model_blob_key"。
        job_id_column (str): 任务id列名，默认为"job_id"。
        resolution_column (str): 分辨率列名，默认为"resolution"。

    InputTables:
        timeline_table: 时间线表格。

    OutputTables:
        timeline_table: 渲染后的时间线表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/base_op/video_render_new_op/video_render_new_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        timeline_table: DataTable = op_context.input_tables[0]
        project_model_builder_column = self.attrs.get("project_model_builder_column", "project_model_builder")
        input_file_map_column = self.attrs.get("input_file_map_column", "input_file_map")
        video_output_blob_key_column = self.attrs.get("video_output_blob_key_column", "video_output_blob_key")
        rpc_status_column = self.attrs.get("rpc_status_column", "rpc_status")
        rpc_message_column = self.attrs.get("rpc_message_column", "rpc_message")
        video_render_res_column = self.attrs.get("video_render_res_column", "video_render_res")
        video_render_param_column = self.attrs.get("video_render_param_column", "video_render_param")
        blob_db = self.attrs.get("blob_db", "ad")
        blob_table = self.attrs.get("blob_table", "nieuwland-material")
        font_map_column = self.attrs.get("font_map_column", "font_map")
        render_biz_name = self.attrs.get("render_biz_name", "nieuwland")
        project_model_blob_key_column = self.attrs.get("project_model_blob_key_column", "project_model_blob_key")
        job_id_column = self.attrs.get("job_id_column", "job_id")
        resolution_column = self.attrs.get("resolution_column", "resolution")
        name_prefix = f"{op_context.request_id}_{get_timestamp()}"

        timeline_table[video_output_blob_key_column] = None
        timeline_table[video_render_param_column] = None
        timeline_table[video_render_res_column] = None
        timeline_table[rpc_status_column] = False
        timeline_table[rpc_message_column] = None
        timeline_table[project_model_blob_key_column] = None
        timeline_table[job_id_column] = None
        for index, row in timeline_table.iterrows():
            project_model_builder: EditSceneVideoProjectBuilder = timeline_table.at[index, project_model_builder_column]
            input_file_map: dict = timeline_table.at[index, input_file_map_column]
            resolution = timeline_table.at[index, resolution_column]
            font_map = None
            if font_map_column in timeline_table.columns:
                font_map = timeline_table.at[index, font_map_column]

            blob_client = BlobStoreClientManager().get_client(f"{blob_db}-{blob_table}")
            project_model_ins = project_model_builder.fetch_model()
            project_model_blob_key = f"{blob_db}_{blob_table}_{name_prefix}_model.pb"
            blob_client.upload_bytes_to_s3(project_model_ins.SerializeToString(), f"{name_prefix}_model.pb")
            input_file_map.update({project_model_blob_key: project_model_blob_key})
            output_blob_key = f"{blob_db}_{blob_table}_{name_prefix}_output.mp4"

            project_composite_instruction = ProjectCompositeInstruction()
            project_composite_instruction.projectPath = project_model_blob_key
            project_composite_instruction.compositeOutputPath = "output.mp4"
            project_composite_instruction.videoRenderType = CompositeRenderType.RENDER_TYPE_VIDEO_RENDER
            video_composite_param = VideoCompositeParam(width=resolution[0], height=resolution[1],
                                                        video_frame_rate=Rational(num=30, den=1),
                                                        video_codec=VideoCodecType.VIDEO_CODEC_H264)
            project_composite_instruction.videoCompositeParam.CopyFrom(video_composite_param)
            project_composite_instruction.segmentMaxDuration = -1
            if resolution[0] == 1080:
                project_composite_instruction.codecParamPreset = CompositeCodecParamPreset.CODEC_PARAM_PRESET_H264_NVENC_NO_BF_1080P_25FPS
            else:
                project_composite_instruction.codecParamPreset = CompositeCodecParamPreset.CODEC_PARAM_PRESET_SL200_X264_720P_25FPS_SPEED0
            if font_map:
                project_composite_instruction.fontId2PathMapping.update(font_map)

            input_file_map_pb = {}
            for mkey, value in input_file_map.items():
                if not mkey or not value:
                    continue
                db, table, key = parse_bbs_resource_id(value)
                input_file_map_pb.update({
                    mkey: {
                        "type": "BLOBSTORE",
                        "db": db,
                        "table": table,
                        "key": key
                    }
                })

            render_params = {
                "inputFileMap": input_file_map_pb,
                "outputFile": {
                    "type": "BLOBSTORE",
                    "db": blob_db,
                    "table": blob_table,
                    "key": f"{name_prefix}_output.mp4"
                },
                "projectCompositeInstruction": MessageToJson(project_composite_instruction,
                                                             use_integers_for_enums=True),
            }

            # logger.debug(f"project_model:{project_model_ins}")
            logger.debug(f"render_params:{render_params}")
            client = ClientManager().get_client_by_name("KsMediaProcessClient")
            res = client.sync_req(render_params, biz_name=render_biz_name)
            if res.get("status") is False:
                self.fail_reason = f"视频渲染失败：{res.get('error_message')}，job_id：{res.get('job_id')}"
                self.trace_log.update({"fail_reason": self.fail_reason})
                op_context.perf_ctx("video_render_op_failed", extra1=res.get('error_message'), extra2=res.get('job_id'))
                return False

            timeline_table.at[index, video_output_blob_key_column] = output_blob_key
            timeline_table.at[index, video_render_res_column] = res
            timeline_table.at[index, video_render_param_column] = json.dumps(render_params, ensure_ascii=False)
            timeline_table.at[index, rpc_status_column] = res.get("status")
            timeline_table.at[index, rpc_message_column] = res.get("error_message")
            timeline_table.at[index, project_model_blob_key_column] = project_model_blob_key
            timeline_table.at[index, job_id_column] = res.get("job_id")

        op_context.output_tables.append(timeline_table)
        return True


op_register.register_op(VideoRenderNewOp) \
    .add_input(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_output(name="timeline_table", type="DataTable", desc="视频渲染表") \
    .add_attr(name="project_model_builder_column", type="str", desc="项目模型构建列名") \
    .add_attr(name="input_file_map_column", type="str", desc="输入文件映射列名") \
    .add_attr(name="video_output_blob_key_column", type="str", desc="视频输出blob地址列名") \
    .add_attr(name="rpc_status_column", type="str", desc="rpc状态列名") \
    .add_attr(name="rpc_message_column", type="str", desc="rpc消息列名") \
    .add_attr(name="video_render_res_column", type="str", desc="视频渲染结果列名") \
    .add_attr(name="video_render_param_column", type="str", desc="视频渲染参数列名") \
    .add_attr(name="blob_db", type="str", desc="blob db名") \
    .add_attr(name="blob_table", type="str", desc="blob table名") \
    .add_attr(name="font_map_column", type="str", desc="字幕字体映射列名") \
    .add_attr(name="render_biz_name", type="str", desc="渲染业务名") \
    .add_attr(name="project_model_blob_key_column", type="str", desc="项目模型blob key列名") \
    .add_attr(name="job_id_column", type="str", desc="job id列名") \
    .add_attr(name="resolution_column", type="str", desc="分辨率列名") \
    .set_parallel(True)
