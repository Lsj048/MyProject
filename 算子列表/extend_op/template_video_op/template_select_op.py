import random
import os

from video_graph import logger
from video_graph.common.client.client_manager import ClientManager
from video_graph.common.utils.kconf import get_kconf_value
from video_graph.common.utils.tools import get_useful_template_id, preprocess_setting_split_key
from video_graph.data_table import DataTable
from video_graph.op import Op, op_register
from video_graph.op_context import OpContext


class TemplateSelectOp(Op):
    """
    【remote】模版选择算子，请求服务获取模版，输出模版ID和模版资源，根据version不同，获取不同的模版

    Attributes:
        first_industry_name_column (str): 一级行业名称列名，默认为"first_industry_name"。
        second_industry_name_column (str): 二级行业名称列名，默认为"second_industry_name"。
        reco_text_column (str): 推荐文案列名，默认为"reco_text"。
        version_column (str): 版本列名，默认为"version"。
        product_name_column (str): 产品名称列名，默认为"product_name"。
        account_id_column (str): 账号ID列名，默认为"account_id"。
        template_id_column (str): 模版ID列名，默认为"template_id"。
        template_res_column (str): 模版资源列名，默认为"template_res"。
        input_version (str): 输入的模版版本。
        vtuber_extra_info_column (str): Vtuber额外信息列名，默认为"vtuber_extra_info"。
        vtuber_name_column (str): Vtuber名称列名，默认为"vtuber_name"。

    InputTables:
        shot_table: 镜头表格。
        request_table: 请求表格。
        vtuber_table: 虚拟人表格。

    OutputTables:
        request_table: 添加了模版ID和模版资源的表格。
        vtuber_table: 添加了模版ID和模版资源的表格。

    Href:
        https://git.corp.kuaishou.com/ad-aigc-algo-engine/video-graph/-/blob/master/video_graph/ops/extend_op/template_video_op/template_select_op.py?ref_type=heads

    Examples:
        #等待作者更新
    """

    def compute(self, op_context: OpContext) -> bool:
        shot_table: DataTable = op_context.input_tables[0]
        request_table: DataTable = op_context.input_tables[1]
        vtuber_table: DataTable = op_context.input_tables[2]
        first_industry_name_column = self.attrs.get("first_industry_name_column", "first_industry_name")
        second_industry_name_column = self.attrs.get("second_industry_name_column", "second_industry_name")
        reco_text_column = self.attrs.get("reco_text_column", "reco_text")
        version_column = self.attrs.get("version_column", "version")
        product_name_column = self.attrs.get("product_name_column", "product_name")
        account_id_column = self.attrs.get("account_id_column", "account_id")
        template_id_column = self.attrs.get("template_id_column", "template_id")
        template_res_column = self.attrs.get("template_res_column", "template_res")
        input_version = self.attrs.get("input_version", None)
        # vtuber_extra_info_column = self.attrs.get("vtuber_extra_info_column", "vtuber_extra_info")
        vtuber_id_column = self.attrs.get("vtuber_id_column", "vtuber_id")

        first_industry_name = request_table.at[0, first_industry_name_column]
        second_industry_name = request_table.at[0, second_industry_name_column]
        reco_text = request_table.at[0, reco_text_column]
        product_name = request_table.at[0, product_name_column]
        account_id = request_table.at[0, account_id_column]
        # 根据请求设置change_title 、need_montage_video 参数
        if input_version == "dsp_virtual_human_v3-final":  # 镜头串联总模版
            need_montage_video = [True, False]
            change_title = False
        elif input_version == "dsp_virtual_human_v1-0_background":  # 根据输入的素材当作数字人背景
            change_title = [True, False]
            need_montage_video = [True, False]
        elif input_version == "dsp_virtual_human_v4-0":  # 实景数字人模版
            change_title = False
            need_montage_video = False
        elif input_version in ["dsp_virtual_human_v1-0", "dsp_virtual_human_v2-0"]:  # 单数字人模版
            if input_version == "dsp_virtual_human_v2-0":
                need_montage_video = [True, False]
            else:
                need_montage_video = False
            if reco_text and request_table.at[0, version_column] in ["dsp_virtual_human_v3-1", 'dsp_virtual_human_v3-5',
                                                                     "dsp_virtual_human_v3-7"]:
                change_title = True
            else:
                change_title = False
        else:
            if input_version in ["dsp_virtual_human_v3-6", "dsp_virtual_human_v3-7"]:
                need_montage_video = False
            else:
                need_montage_video = True
            if reco_text:
                change_title = True
            else:
                change_title = False

        virtual_human_kconf = get_kconf_value("ad.algorithm.VirtualHumanV3", "json")
        # 获取通用配置
        #virtual_human_info = virtual_human_kconf.get("virtual_human_info", {})
        exclusive_template_id = preprocess_setting_split_key(virtual_human_kconf.get("exclusive_template_id", {}))
        #exclusive_virtual_human = preprocess_setting_split_key(virtual_human_kconf.get("exclusive_virtual_human", {}))
        # 根据请求获取相应配置
        account_setting = preprocess_setting_split_key(virtual_human_kconf.get("setting_for_account", {})).get(
            account_id, {})
        product_setting = preprocess_setting_split_key(virtual_human_kconf.get("setting_for_product", {})).get(
            product_name, {})
        second_industry_setting = preprocess_setting_split_key(
            virtual_human_kconf.get("setting_for_second_industry", {})).get(second_industry_name, {})
        first_industry_setting = preprocess_setting_split_key(
            virtual_human_kconf.get("setting_for_first_industry", {})).get(first_industry_name, {})

        template_id_list = []
        template_res_list = []
        real_scene_template_id_list = []
        real_scene_template_res_list = []
        get_templates_client = ClientManager().get_client_by_name("GetTemplatesClient")
        resp = get_templates_client.sync_req(first_industry_name, second_industry_name, input_version)
        if resp and resp.get("status") and len(resp.get("templates")) > 0:
            template_list = resp.get("templates")
            tmp_template_id_list, tmp_template_res_list = get_useful_template_id(
                template_list, account_id, product_name, reco_text, change_title, need_montage_video
            )
            template_id_list.extend(tmp_template_id_list)
            template_res_list.extend(tmp_template_res_list)
        if input_version in ["dsp_virtual_human_v1-0", "dsp_virtual_human_v2-0", "dsp_virtual_human_v1-0_background"]:
            resp = get_templates_client.sync_req(first_industry_name, second_industry_name, input_version,
                                                 product_name)
            if resp and resp.get("status") and len(resp.get("templates")) > 0:
                template_list = resp.get("templates")
                tmp_template_id_list, tmp_template_res_list = get_useful_template_id(
                    template_list, account_id, product_name, reco_text, change_title, need_montage_video
                )
                template_id_list.extend(tmp_template_id_list)
                template_res_list.extend(tmp_template_res_list)
        # 获取实景数字人的占位模版
        resp = get_templates_client.sync_req(first_industry_name, second_industry_name, 'dsp_virtual_human_v4-0')
        if resp and resp.get("status") and len(resp.get("templates")) > 0:
            template_list = resp.get("templates")
            tmp_template_id_list, tmp_template_res_list = get_useful_template_id(
                template_list, account_id, product_name, reco_text, False, False
            )
            real_scene_template_id_list.extend(tmp_template_id_list)
            real_scene_template_res_list.extend(tmp_template_res_list)

        #  如果有白名单命中则根据白名单in
        if account_setting.get("white", {}).get("template_id", []):
            resource_id_list = []
            resource_index_list = []
            for tmp_resource_id in account_setting.get("white", {}).get("template_id", []):
                if tmp_resource_id in template_id_list:
                    resource_id_list.append(tmp_resource_id)
                    resource_index_list.append(template_res_list[template_id_list.index(tmp_resource_id)])
        elif product_setting.get("white", {}).get("template_id", []):
            resource_id_list = []
            resource_index_list = []
            for tmp_resource_id in product_setting.get("white", {}).get("template_id", []):
                if tmp_resource_id in template_id_list:
                    resource_id_list.append(tmp_resource_id)
                    resource_index_list.append(template_res_list[template_id_list.index(tmp_resource_id)])
        elif second_industry_setting.get("white", {}).get("template_id", []):
            resource_id_list = []
            resource_index_list = []
            for tmp_resource_id in second_industry_setting.get("white", {}).get("template_id", []):
                if tmp_resource_id in template_id_list:
                    resource_id_list.append(tmp_resource_id)
                    resource_index_list.append(template_res_list[template_id_list.index(tmp_resource_id)])
        else:
            resource_id_list = []
            resource_index_list = []
            for tmp_resource_id in first_industry_setting.get("white", {}).get("template_id", []):
                if tmp_resource_id in template_id_list:
                    resource_id_list.append(tmp_resource_id)
                    resource_index_list.append(template_res_list[template_id_list.index(tmp_resource_id)])
        if len(resource_id_list) > 0:
            template_id_list = resource_id_list
            template_res_list = resource_index_list

        # 根据通用配置筛选
        clear_template_id_list = []
        clear_template_res_list = []
        for resource_id, template_res_info in zip(template_id_list, template_res_list):
            if resource_id in exclusive_template_id:
                """
                if (
                        (account_id not in exclusive_template_id[resource_id].get("account", [])) and
                        (product_name not in exclusive_template_id[resource_id].get("product", [])) and
                        (second_industry_name not in exclusive_template_id[resource_id].get("second_industry", [])) and
                        (first_industry_name not in exclusive_template_id[resource_id].get("first_industry", []))
                ):
                    continue
                """
                if account_id not in exclusive_template_id[resource_id].get("account", []) and \
                        product_name not in exclusive_template_id[resource_id].get("product", []) and \
                        second_industry_name not in exclusive_template_id[resource_id].get("second_industry", []) and \
                        first_industry_name not in exclusive_template_id[resource_id].get("first_industry", []):
                    continue
            clear_template_id_list.append(resource_id)
            clear_template_res_list.append(template_res_info)
        if len(clear_template_id_list) == 0:
            logger.info(f"clear_template_id_list is empty，input_version: {input_version}")
            return False
        # 找到所有实景数字人
        real_scene_vtuber_list = []
        for idx, row in shot_table.iterrows():
            if row.get(vtuber_id_column) in virtual_human_kconf.get("real_scene_vtuber_list", []):
                real_scene_vtuber_list.append(row.get(vtuber_id_column))
        if len(real_scene_vtuber_list) > 0 and len(real_scene_template_id_list) == 0:
            logger.info(f"real_scene_template_id_list is empty，using real vtuber human {real_scene_vtuber_list}")
            return False
        if input_version in ["dsp_virtual_human_v1-0", "dsp_virtual_human_v2-0", "dsp_virtual_human_v4-0",
                             "dsp_virtual_human_v1-0_background"]:
            # 数字人的背景板
            if template_id_column not in vtuber_table:
                vtuber_table[template_id_column] = None
            if template_res_column not in vtuber_table:
                vtuber_table[template_res_column] = None
            vtuber_number = len(vtuber_table)
            combined = list(zip(clear_template_id_list, clear_template_res_list))
            random.shuffle(combined)
            clear_template_id_list, clear_template_res_list = zip(*combined)
            for i in range(vtuber_number):
                if vtuber_table.at[i, vtuber_id_column] in real_scene_vtuber_list:
                    vtuber_table.at[i, template_id_column] = real_scene_template_id_list[
                        int(i % len(real_scene_template_id_list))]
                    vtuber_table.at[i, template_res_column] = real_scene_template_res_list[
                        int(i % len(real_scene_template_res_list))]
                else:
                    vtuber_table.at[i, template_id_column] = clear_template_id_list[
                        int(i % len(clear_template_id_list))]
                    vtuber_table.at[i, template_res_column] = clear_template_res_list[int(i % len(clear_template_res_list))]
        else:
            # 镜头串联总模版或镜头模版
            template_index = random.randint(0, len(clear_template_id_list) - 1)
            template_id = clear_template_id_list[template_index]
            template_res = clear_template_res_list[template_index]

            request_table[template_id_column] = None
            request_table[template_res_column] = None
            request_table.at[0, template_id_column] = template_id
            request_table.at[0, template_res_column] = template_res

        op_context.output_tables.append(request_table)
        op_context.output_tables.append(vtuber_table)
        return True

op_register.register_op(TemplateSelectOp) \
    .add_input(name="shot_table", type="DataTable", desc="镜号表") \
    .add_input(name="request_table", type="DataTable", desc="请求表") \
    .add_input(name="vtuber_table", type="DataTable", desc="请求表") \
    .add_output(name="request_table", type="DataTable", desc="请求表") \
    .add_output(name="vtuber_table", type="DataTable", desc="虚拟人表")
