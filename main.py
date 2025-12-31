# 支持视频+音频指纹，支持获取时间线
import sys, os, json, re, subprocess, numpy as np, datetime, requests, time, csv
from bs4 import BeautifulSoup
from itertools import accumulate
from concurrent.futures import ThreadPoolExecutor

from video_config import Config, auraprint_log

# 需要下载yt-dlp
# pip install yt-dlp

# 只识别720p的视频
VIDEO_MP4_ITAG = [136, 298, 398]
VIDEO_WEBM_ITAG = [247, 302, 334]
AUDIO_WEBM_ITAG = [251, "251-drc"]
AUDIO_MP4_ITAG = []


# 定义参考对象的类
class Reference():
    def __init__(self, Reference_Type, Reference_Size, Subsegment_Duration, Starts_with_SAP, SAP_Type):
        # 初始化参考属性
        self.Reference_Type = Reference_Type
        self.Reference_Size = Reference_Size
        self.Subsegment_Duration = Subsegment_Duration
        self.Starts_with_SAP = Starts_with_SAP
        self.SAP_Type = SAP_Type


# 定义轨道信息的类
class Track():
    # 初始化轨道属性
    def __init__(self, Track_Time, Track_Number, Track_Position):
        self.Track_Time = Track_Time
        self.Track_Number = Track_Number
        self.Track_Position = Track_Position


# 定义处理视频或音频分段元数据的类
class Box():
    def __init__(self, itag, start, end, video_name, down_path):
        # 初始化ITAG和路径相关属性
        self.itag = itag
        self.start = start
        self.end = end
        self.video_mp4_itag = VIDEO_MP4_ITAG
        self.video_webm_itag = VIDEO_WEBM_ITAG
        self.audio_webm_itag = AUDIO_WEBM_ITAG
        self.audio_mp4_itag = AUDIO_MP4_ITAG
        # 根据ITAG选择适当的解析方法
        if self.itag in (self.video_mp4_itag + self.audio_mp4_itag):
            self.get_metedata_mp4(video_name, down_path)
        elif self.itag in (self.video_webm_itag + self.audio_webm_itag):
            self.get_metedata_webm(video_name, down_path)

        # else:
        #     raise ValueError('Itag Wrong')

    # 解析MP4格式元数据
    def get_metedata_mp4(self, video_name, down_path):
        # 检查文件是否存在，读取头部数据
        videopath = down_path + r'video/{}/{}_{}.mp4'.format(video_name, video_name, self.itag)
        if os.path.exists(videopath):
            with open(videopath, 'rb') as f:
                header_data = f.read(10000)
        elif os.path.exists(videopath + '.part'):
            with open(videopath + '.part', 'rb') as f:
                header_data = f.read(10000)
        else:
            auraprint_log(
                f"【解析MP4格式元数据未下载该视频】{video_name} {str(self.itag)} 文件不存在",
                Config.FINGERPRINT_LOG, "warning")
            # 设置标志，表示文件不存在
            self.file_not_found = True
            return

        # 从头部数据中提取索引范围并解析元数据
        sidx = header_data[self.start:self.end + 1]  # index_range
        # index_range包含以下信息+各片段信息
        self.Box_Size = int.from_bytes(sidx[:4], byteorder='big')
        sidx = sidx[4:]
        self.Box_Type = int.from_bytes(sidx[:4], byteorder='big')
        sidx = sidx[4:]
        self.Version = int.from_bytes(sidx[:1], byteorder='big')
        sidx = sidx[1:]
        self.Flags = int.from_bytes(sidx[:3], byteorder='big')
        sidx = sidx[3:]
        self.Reference_ID = int.from_bytes(sidx[:4], byteorder='big')
        sidx = sidx[4:]
        self.Timescale = int.from_bytes(sidx[:4], byteorder='big')
        sidx = sidx[4:]

        # 根据版本解析不同的时间和偏移信息
        if self.Version == 0:
            self.Earliest_Presentation_Time = int.from_bytes(sidx[:4], byteorder='big')
            sidx = sidx[4:]
            self.First_Offset = int.from_bytes(sidx[:4], byteorder='big')
            sidx = sidx[4:]
        elif self.Version == 1:
            self.Earliest_Presentation_Time = int.from_bytes(sidx[:8], byteorder='big')
            sidx = sidx[8:]
            self.First_Offset = int.from_bytes(sidx[:8], byteorder='big')
            sidx = sidx[8:]
        else:
            # 记录版本错误日志
            auraprint_log(
                f"【解析MP4格式元数据版本错误】  {video_name}  {str(self.itag)}  {str(self.Version)}",
                Config.FINGERPRINT_LOG, "warning")
            self.Earliest_Presentation_Time = int.from_bytes(sidx[:4], byteorder='big')
            sidx = sidx[4:]
            self.First_Offset = int.from_bytes(sidx[:4], byteorder='big')
            sidx = sidx[4:]
            # raise Exception('Version Inexistence')

        # 解析保留字段和参考计数
        self.Reserved = int.from_bytes(sidx[:2], byteorder='big')
        sidx = sidx[2:]
        self.Reference_Count = int.from_bytes(sidx[:2], byteorder='big')
        sidx = sidx[2:]

        # 循环解析每个参考
        self.reference = []
        self.reference_list = []
        self.duration_list = []
        while len(sidx) != 0:
            Reference_Type = int.from_bytes(sidx[:1], byteorder='big')
            sidx = sidx[1:]
            Reference_Size = int.from_bytes(sidx[:3], byteorder='big')
            sidx = sidx[3:]
            Subsegment_Duration = int.from_bytes(sidx[:4], byteorder='big')
            sidx = sidx[4:]
            Starts_with_SAP = int.from_bytes(sidx[:1], byteorder='big')
            sidx = sidx[1:]
            SAP_Type = int.from_bytes(sidx[:3], byteorder='big')
            sidx = sidx[3:]

            ref = Reference(Reference_Type, Reference_Size, Subsegment_Duration, Starts_with_SAP, SAP_Type)
            self.reference.append(ref)
            self.reference_list.append(Reference_Size)
            self.duration_list.append(Subsegment_Duration)

    # 解析WebM格式元数据
    def get_metedata_webm(self, video_name, down_path):
        videopath = down_path + r'/video/{}/{}_{}.webm'.format(video_name, video_name, self.itag)
        if os.path.exists(videopath):
            with open(videopath, 'rb') as f:
                header_data = f.read(10000)
        elif os.path.exists(videopath + '.part'):
            with open(videopath + '.part', 'rb') as f:
                header_data = f.read(10000)
        else:
            auraprint_log(
                f"【解析WebM格式元数据未下载该视频】 {video_name}  {str(self.itag)} 文件不存在",
                Config.FINGERPRINT_LOG, "warning")
            # 设置标志，表示文件不存在
            self.file_not_found = True
            return
        cues = header_data[self.start:self.end + 1]

        self.Cues_Header = cues[:6]
        cues = cues[6:]

        self.track = []
        self.track_list = []
        self.timeline = []
        while len(cues) != 0:
            Track_Time_Flag = int.from_bytes(cues[3:4], byteorder='big')
            cues = cues[4:]
            Track_Time_Length = Track_Time_Flag - 0x80
            Track_Time = int.from_bytes(cues[:Track_Time_Length], byteorder='big')  # ms timescale默认1000？
            cues = cues[Track_Time_Length:]

            Track_Number_Flag = int.from_bytes(cues[3:4], byteorder='big')
            cues = cues[4:]
            Track_Number_Length = Track_Number_Flag - 0x80
            Track_Number = int.from_bytes(cues[:Track_Number_Length], byteorder='big')
            cues = cues[Track_Number_Length:]

            Track_Position_Flag = int.from_bytes(cues[1:2], byteorder='big')
            Track_Position_Length = Track_Position_Flag - 0x80
            cues = cues[2:]

            Track_Position = int.from_bytes(cues[:Track_Position_Length], byteorder='big')
            cues = cues[Track_Position_Length:]

            tra = Track(Track_Time, Track_Number, Track_Position)
            self.track.append(tra)
            if len(self.track) > 1:
                self.track_list.append(self.track[-1].Track_Position - self.track[-2].Track_Position)
            self.timeline.append(Track_Time)


class Video():
    def __init__(self, ID, url):
        self.ID = ID
        self.url = url
        self.video_name = self.url.split('=')[1]

        self.video_mp4_itag = VIDEO_MP4_ITAG
        self.video_webm_itag = VIDEO_WEBM_ITAG
        self.audio_webm_itag = AUDIO_WEBM_ITAG
        self.audio_mp4_itag = AUDIO_MP4_ITAG

        # self.cookie_txt = COOKIE_TXT
        self.down_path = Config.FINGERPRINT_DOWN

    def get_websource(self):
        # auraprint_log(f"【开始get websource】 {self.url} ",Config.FINGERPRINT_LOG,"debug")
        if not os.path.exists(self.down_path + r'websource/'):
            os.makedirs(self.down_path + r'websource/')
        response_path = self.down_path + r'websource/' + self.video_name + '.html'
        # 避免下载失败/重复下载，限制重试次数
        retry_count = 0
        max_retries = Config.WEBSOURCE_MAX_RETRIES
        while not os.path.exists(response_path) and retry_count < max_retries:
            # 方案1
            # auraprint_log(f"【开始下载 websource】 {self.url} ",Config.FINGERPRINT_LOG,"debug")
            # response = requests.get(self.url)#请求
            # if response.status_code == 200:
            #     with open(self.down_path + r'websource/' + self.video_name + '.html', 'w', encoding='utf-8') as f:
            #         f.write(response.text)
            retry_count += 1
            try:
                # 开始记录请求时间
                start_time = time.time()
                auraprint_log(f"【开始下载 websource】 {self.video_name} 第 {retry_count}/{max_retries} 次尝试 URL: {self.url}", 
                               Config.FINGERPRINT_LOG, "debug")

                # 设置超时时间
                response = requests.get(self.url, timeout=Config.WEBSOURCE_TIMEOUT)

                # 检查请求状态
                if response.status_code == 200:
                    elapsed_time = time.time() - start_time  # 计算请求耗时
                    auraprint_log(f"【websource下载完成】耗时 {elapsed_time:.2f} 秒 {self.url}",
                                     Config.FINGERPRINT_LOG, "info")

                    # 保存下载的网页内容
                    with open(self.down_path + r'websource/' + self.video_name + '.html', 'w', encoding='utf-8') as f:
                        f.write(response.text)
                    break  # 下载成功，退出循环
                else:
                    auraprint_log(f"【请求失败】 {self.video_name} 状态码: {response.status_code} URL: {self.url} (第 {retry_count}/{max_retries} 次)",
                                     Config.FINGERPRINT_LOG, "error")

            except requests.exceptions.Timeout:
                auraprint_log(f"【请求超时】 {self.video_name} 超过 {Config.WEBSOURCE_TIMEOUT} 秒未响应 URL: {self.url} (第 {retry_count}/{max_retries} 次)", 
                               Config.FINGERPRINT_LOG, "error")
            except requests.exceptions.RequestException as e:
                auraprint_log(f"【请求异常】 {self.video_name} URL: {self.url} 错误: {e} (第 {retry_count}/{max_retries} 次)", 
                               Config.FINGERPRINT_LOG, "error")
        
        # 检查是否达到最大重试次数
        if retry_count >= max_retries and not os.path.exists(response_path):
            auraprint_log(f"【websource下载最终失败】 {self.video_name} 已达到最大重试次数({max_retries}次) URL: {self.url}", 
                           Config.FINGERPRINT_LOG, "warning")
        elif os.path.exists(response_path):
            auraprint_log(f"【Websource 已存在】 {self.video_name} ", Config.FINGERPRINT_LOG, "debug")

    def analyse_websource(self):
        html_path = self.down_path + r'websource/' + self.video_name + '.html'
        with open(html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tags = soup.find_all('script')  # 找到所有script标签
        pattern = re.compile(r'var\s+ytInitialPlayerResponse\s*=\s*({.*?});', re.DOTALL)

        for script_tag in script_tags:
            script_content = ''.join(map(str, script_tag.contents))  # contents转为一个完整长字符串
            match = pattern.search(script_content)
            if match:
                javascript_code = match.group(1)
        data = json.loads(javascript_code)
        service_tracking_params = data.get('streamingData', {}).get('adaptiveFormats', [])

        self.itag_list = []
        self.itag_quality = {}
        self.itag_vcodec = {}
        self.itag_indexrange = {}
        self.itag_contentlength = {}
        # 使用集合跟踪已添加的itag，避免重复
        added_itags = set()
        
        for param in service_tracking_params:
            itag = param.get('itag')
            is_drc = param.get('isDrc')  # None/True
            if is_drc:
                itag = str(itag) + "-drc"

            # 检查itag是否已添加，避免重复
            if itag in added_itags:
                auraprint_log(f"【跳过重复itag】 {self.video_name}  {str(itag)} 在adaptiveFormats中重复出现，跳过",
                               Config.FINGERPRINT_LOG, "debug")
                continue

            # print("【itag】",itag)

            if itag in (self.video_mp4_itag + self.video_webm_itag):
                indexRange = param.get('indexRange', {'start': 0, 'end': 0})  # modify
                if indexRange == {'start': 0, 'end': 0}:
                    auraprint_log(
                        f"【解析websourse该itag没有indexRange】  {self.video_name}  {str(itag)} ",
                        Config.FINGERPRINT_LOG, "warning")
                else:
                    self.itag_list.append(itag)  # 只下载有IndexRange的
                    added_itags.add(itag)  # 标记为已添加

                    width = param['width']
                    height = param['height']
                    quality = str(width) + 'x' + str(height)
                    self.itag_quality[itag] = quality  #

                    codecs = param['mimeType'].split("\"")[1].split('.')[0]  # 'video/mp4; codecs="avc1.640028"'
                    self.itag_vcodec[itag] = codecs

                    indexRange['start'] = int(indexRange['start'])
                    indexRange['end'] = int(indexRange['end'])
                    self.itag_indexrange[itag] = indexRange

                    self.itag_contentlength[itag] = int(param.get('contentLength', 0))

            elif itag in (self.audio_mp4_itag + self.audio_webm_itag):
                indexRange = param.get('indexRange', {'start': 0, 'end': 0})  # modify
                if indexRange == {'start': 0, 'end': 0}:
                    auraprint_log(
                        f"【解析websourse该itag不合法的indexRange】 {self.video_name}  {str(itag)} ",
                        Config.FINGERPRINT_LOG, "warning")
                else:
                    self.itag_list.append(itag)  # 只下载有IndexRange的
                    added_itags.add(itag)  # 标记为已添加

                    quality = param['quality']
                    self.itag_quality[itag] = quality  #

                    codecs = param['mimeType'].split("\"")[1].split('.')[0]
                    self.itag_vcodec[itag] = codecs

                    indexRange['start'] = int(indexRange['start'])
                    indexRange['end'] = int(indexRange['end'])
                    self.itag_indexrange[itag] = indexRange

                    self.itag_contentlength[itag] = int(param.get('contentLength'))

    def analyse_video(self):
        self.itag_box = {}
        fingerprint_list = []
        # auraprint_log(f"【正在解析指纹】",Config.FINGERPRINT_LOG,"debug")

        # 去重：确保每个itag只处理一次
        processed_itags = set()
        for itag in self.itag_list:
            # 如果itag已经处理过，跳过
            if itag in processed_itags:
                auraprint_log(f"【跳过重复itag】 {self.video_name}  {itag} 已处理过，跳过",
                               Config.FINGERPRINT_LOG, "debug")
                continue
            processed_itags.add(itag)
            start, end = self.itag_indexrange[itag]['start'], self.itag_indexrange[itag]['end']
            ##################获取一个itag的视频指纹##################
            box = Box(itag, start, end, self.video_name, self.down_path)
            quality = self.itag_quality[itag]
            vcodec = self.itag_vcodec[itag]
            contentLength = self.itag_contentlength[itag]
            self.itag_box[itag] = box
            
            # 检查文件是否存在（如果文件不存在，box会有file_not_found属性）
            if hasattr(box, 'file_not_found') and box.file_not_found:
                auraprint_log(f"【跳过解析指纹】 {self.video_name}  {itag} 原因: 文件不存在",
                               Config.FINGERPRINT_LOG, "warning")
                continue
            
            if hasattr(box, 'reference_list'):
                duration_list = [1000 * x // box.Timescale for x in box.duration_list]
                timeline = [0] + list(accumulate(duration_list))
                condition = (contentLength == end + 1 + sum(box.reference_list))
                auraprint_log(
                    f"【正在解析指纹】{itag:<9}, {self.itag_contentlength[itag]:<12}, 'fmp4', {end + 1 + sum(box.reference_list):<12}, {(condition if condition == False else ''):<8}, {contentLength - (end + 1 + sum(box.reference_list))}",
                    Config.FINGERPRINT_LOG, "debug")
                if itag != 140 or (itag == 140 and condition):
                    fingerprint_list.append(
                        [self.ID, self.url, itag, quality, 'fmp4', vcodec, start, end, contentLength,
                         '/'.join(map(str, box.reference_list)), box.Timescale,
                         '/'.join(map(str, box.duration_list)), '/'.join(map(str, timeline))])
            elif hasattr(box, 'timeline'):
                duration_list = (np.diff(box.timeline)).tolist()
                fingerprint_list.append([self.ID, self.url, itag, quality, 'webm', vcodec, start, end, contentLength,
                                         '/'.join(map(str, box.track_list)), '1000', '/'.join(map(str, duration_list)),
                                         '/'.join(map(str, box.timeline))])
                auraprint_log(
                    f"【正在解析指纹】{itag:<9}, {self.itag_contentlength[itag]:<12}, 'webm', {end + 1 + sum(box.track_list):<12}, {(contentLength == end + 1 + sum(box.track_list)):<8}, {contentLength - (end + 1 + sum(box.track_list))}",
                    Config.FINGERPRINT_LOG, "debug")
            else:
                auraprint_log(f"【解析指纹失败】 {self.video_name}  {itag} 原因: 无法识别格式或元数据解析失败",
                               Config.FINGERPRINT_LOG, "warning")
                continue
        return fingerprint_list

    def download_video(self, itag, ITAG_DL_TIMEOUT, MIN_ITAG_DL_SIZE):
        # 创建保存视频文件的目录
        video_dir = os.path.join(self.down_path, r'video', self.video_name)

        # 确保目录存在，如果不存在则创建
        os.makedirs(video_dir, exist_ok=True)

        # 确定视频文件路径
        if itag in self.video_mp4_itag or itag in self.audio_mp4_itag:
            videopath = os.path.join(video_dir, f"{self.video_name}_{itag}.mp4")
        elif itag in self.video_webm_itag or itag in self.audio_webm_itag:
            videopath = os.path.join(video_dir, f"{self.video_name}_{itag}.webm")

        # 如果文件不存在且没有.part文件，开始下载
        auraprint_log(f"【开始下载】{self.video_name} {itag}", Config.FINGERPRINT_LOG, "debug")
        
        # 使用 Python 模块方式调用 yt-dlp（更可靠）

        command = [
            sys.executable, '-m', 'yt_dlp',
            '--limit-rate', '1M',
            '-f', str(itag),
            self.url,
            '-o', videopath
        ]

        # 每个线程任务中，启动一个下载进程
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            stdout, stderr = process.communicate(timeout=ITAG_DL_TIMEOUT)  # 等待ITAG_DL_TIMEOUT秒后结束
        except subprocess.TimeoutExpired:
            process.terminate()
            stdout, stderr = process.communicate()  #
            auraprint_log(f"【下载失败】{self.video_name} {itag}：下载超时（{ITAG_DL_TIMEOUT}秒），进程被终止。",
                             Config.FINGERPRINT_LOG, "debug")
        except Exception as e:
            auraprint_log(f"【下载失败】{self.video_name} {itag} 错误：{e}", Config.FINGERPRINT_LOG, "error")

        # 检查下载是否成功，失败/小于MIN_ITAG_DL_SIZE则删掉
        if (os.path.exists(videopath + '.part') and os.path.getsize(videopath + '.part') >= MIN_ITAG_DL_SIZE) or \
                (os.path.exists(videopath) and os.path.getsize(videopath) >= MIN_ITAG_DL_SIZE):
            auraprint_log(f"【下载完成】{videopath}(.part)", Config.FINGERPRINT_LOG, "info")
            return videopath
        else:
            # 如果文件未下载完成或文件小于规定的最小大小，删除文件
            for path in [videopath + '.part', videopath]:
                if os.path.exists(path):
                    os.remove(path)  # 删除文件
                    auraprint_log(f"【删除文件】{videopath}(.part) 文件大小小于 {MIN_ITAG_DL_SIZE} 字节",
                                     Config.FINGERPRINT_LOG, "debug")

            # 如果文件未成功下载，记录下载失败的详细信息
            if os.path.exists(videopath + '.part'):
                auraprint_log(f"【下载失败】{videopath}(.part) 文件存在，但未成功下载或被中断", Config.FINGERPRINT_LOG,
                                 "warning")
            elif os.path.exists(videopath):
                auraprint_log(f"【下载失败】{videopath} 文件已存在，但大小小于 {MIN_ITAG_DL_SIZE} 字节",
                                 Config.FINGERPRINT_LOG, "warning")
            else:
                auraprint_log(f"【下载失败】{videopath} 未下载或文件不存在", Config.FINGERPRINT_LOG, "warning")

            return None


def batch_dl_video_header(video_list, MAX_THREADS, ITAG_DL_TIMEOUT, MIN_ITAG_DL_SIZE, MAX_RETRIES=3):
    """
    批量处理视频下载，支持有限次重试
    
    Args:
        video_list: 视频对象列表
        MAX_THREADS: 最大线程数
        ITAG_DL_TIMEOUT: 下载超时时间
        MIN_ITAG_DL_SIZE: 最小文件大小
        MAX_RETRIES: 最大重试次数（默认3次）
    """
    retries = {}  # 用于存储需要重新处理的视频对象及其重试次数 {video: retry_count}

    while video_list:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            tasks = []
            current_retries = {}  # 本轮需要重试的视频

            # 处理当前的视频列表
            for video in video_list:
                # 检查是否已达到最大重试次数
                if video in retries and retries[video] > MAX_RETRIES:
                    auraprint_log(f"【跳过重试】视频 {video.url} 已达到最大重试次数({MAX_RETRIES}次)", 
                                   Config.FINGERPRINT_LOG, "warning")
                    continue
                
                # 检查websource是否已存在且已解析
                websource_path = video.down_path + r'websource/' + video.video_name + '.html'
                is_retry = video in retries  # 判断是否为重试
                
                # 如果是第一次处理，需要下载和解析websource
                if not is_retry:
                    video.get_websource()
                    video.analyse_websource()
                else:
                    # 重试时，如果websource文件已存在，只需解析（不重新下载）
                    # 如果itag_list已有值，说明已解析过，跳过解析
                    if os.path.exists(websource_path):
                        if not hasattr(video, 'itag_list') or not video.itag_list:
                            # websource存在但未解析，需要解析
                            auraprint_log(f"【重试解析websource】{video.video_name} websource已存在，重新解析", 
                                           Config.FINGERPRINT_LOG, "debug")
                            video.analyse_websource()
                        else:
                            # websource已存在且已解析，直接使用已有的itag_list
                            auraprint_log(f"【跳过解析websource】{video.video_name} 使用已解析的itag列表（{len(video.itag_list)}个itag）", 
                                           Config.FINGERPRINT_LOG, "debug")
                    else:
                        # 重试时websource不存在，需要重新下载
                        auraprint_log(f"【重试下载websource】{video.video_name} websource不存在，重新下载", 
                                       Config.FINGERPRINT_LOG, "warning")
                        video.get_websource()
                        video.analyse_websource()

                for itag in video.itag_list:
                    video_dir = os.path.join(video.down_path, 'video', video.video_name)

                    if itag in VIDEO_MP4_ITAG or itag in AUDIO_MP4_ITAG:
                        videopath = os.path.join(video_dir, f"{video.video_name}_{itag}.mp4")
                    elif itag in VIDEO_WEBM_ITAG or itag in AUDIO_WEBM_ITAG:
                        videopath = os.path.join(video_dir, f"{video.video_name}_{itag}.webm")

                    if os.path.exists(videopath + '.part') and os.path.getsize(
                            videopath + '.part') >= MIN_ITAG_DL_SIZE or os.path.exists(videopath) and os.path.getsize(
                        videopath) >= MIN_ITAG_DL_SIZE:
                        # print(f"{itag}.part exists.")
                        auraprint_log(f"【下载跳过】{video.video_name} {itag}.part 已存在.", Config.FINGERPRINT_LOG, "debug")

                        continue  # good文件已存在，跳过下载任务

                    tasks.append((video, executor.submit(video.download_video, itag, ITAG_DL_TIMEOUT, MIN_ITAG_DL_SIZE)))  # 提交任务
                    # print(f"add {itag}.")
                    auraprint_log(f"【增加itag】{video.video_name} {itag}", Config.FINGERPRINT_LOG, "debug")

            # 等待所有线程完成
            for video, task in tasks:
                try:
                    videopath = task.result()  # 会等待task结
                    if videopath is None:
                        # 记录失败，准备重试
                        if video not in current_retries:
                            current_retries[video] = True
                        auraprint_log(f"【下载失败】 {video.video_name} 的某个itag下载失败，将重试", 
                                       Config.FINGERPRINT_LOG, "debug")

                except Exception as e:
                    # 记录失败，准备重试
                    if video not in current_retries:
                        current_retries[video] = True
                    auraprint_log(f"【下载失败】 {video.video_name} 下载异常: {e}，将重试", 
                                   Config.FINGERPRINT_LOG, "error")

        # 更新重试次数并准备下一轮
        next_retry_list = []
        for video in current_retries:
            # 增加重试次数
            if video not in retries:
                retries[video] = 0
            retries[video] += 1
            
            # 如果未达到最大重试次数，加入下一轮重试列表
            if retries[video] <= MAX_RETRIES:
                next_retry_list.append(video)
                auraprint_log(f"【准备重试】 {video.video_name} 第 {retries[video]} 次重试", 
                               Config.FINGERPRINT_LOG, "info")
            else:
                auraprint_log(f"【下载最终失败】 {video.video_name} 已达到最大重试次数({MAX_RETRIES}次)，停止重试", 
                               Config.FINGERPRINT_LOG, "warning")
        
        # 更新video_list为需要重试的视频
        video_list = next_retry_list
        
        if video_list:
            auraprint_log(f"【开始下一轮重试】剩余 {len(video_list)} 个视频需要重试", 
                           Config.FINGERPRINT_LOG, "info")

    # 统计最终结果
    failed_count = sum(1 for count in retries.values() if count > MAX_RETRIES)
    if failed_count > 0:
        auraprint_log(f"【部分下载失败】有 {failed_count} 个视频下载失败，已达到最大重试次数({MAX_RETRIES}次)", 
                       Config.FINGERPRINT_LOG, "warning")
    else:
        auraprint_log(f"【全部下载完成】", Config.FINGERPRINT_LOG, "info")


def batch_get_fingerprints(video_list):
    """
    批量处理多个视频的完整流程：下载、分析、写入CSV
    
    Args:
        video_list: 视频对象列表
        
    Returns:
        dict: {video.url: flag} 每个视频的处理结果
    """
    # 1. 批量下载视频
    batch_dl_video_header(video_list, Config.MAX_THREADS, Config.ITAG_DL_TIMEOUT, Config.MIN_ITAG_DL_SIZE, Config.MAX_RETRIES)
    
    # 2. 批量分析指纹
    all_fingerprint_data = []  # 存储所有视频的指纹数据 [(video, fingerprint_list), ...]
    for video in video_list:
        try:
            fingerprint_list = video.analyse_video()  # 分析文件
            all_fingerprint_data.append((video, fingerprint_list))
            auraprint_log(f"【指纹分析完成】{video.video_name} 分析完成", Config.FINGERPRINT_LOG, "debug")
        except Exception as e:
            auraprint_log(f"【指纹分析失败】{video.video_name} 分析出错: {e}", Config.FINGERPRINT_LOG, "error")
            all_fingerprint_data.append((video, []))
    
    # 3. 批量写入CSV
    # 检查文件是否存在，如果不存在或为空则写入表头
    file_exists = os.path.exists(Config.FINGERPRINT_FILE)
    write_header = False
    if not file_exists or os.path.getsize(Config.FINGERPRINT_FILE) == 0:
        write_header = True
    
    results = {}  # {url: flag}
    pattern = r'^\d+x\d+$'  # 定义一个正则表达式，匹配视频质量的格式，如1280x720
    
    with open(Config.FINGERPRINT_FILE, 'a', newline='', encoding='utf-8') as processed_file:
        writer = csv.writer(processed_file)
        # 如果需要写入表头，先写入表头
        if write_header:
            header = ['ID', 'url', 'video_itag', 'video_quality', 'video_format', 
                     'audio_itag', 'audio_quality', 'audio_format', 
                     'video_fp', 'video_timeline', 'audio_fp', 'audio_timeline']
            writer.writerow(header)
        
        # 处理每个视频的指纹数据
        for video, fingerprint_list in all_fingerprint_data:
            # 指纹混合：分离视频和音频指纹
            current_category_video = []
            current_category_audio = []
            for row in fingerprint_list:
                if re.match(pattern, row[3]):  # 假设视频质量在第4列
                    current_category_video.append(row)  # 如果是视频质量，初始化视频指纹数据
                else:
                    current_category_audio.append(row)  # 如果不是视频质量，初始化音频指纹数据
            
            # 每个视频独立去重：使用集合记录该视频内已写入的组合，避免同一视频内重复写入
            written_combinations = set()
            written_count = 0  # 记录该视频实际写入的行数
            
            for video_row in current_category_video:
                for audio_row in current_category_audio:
                    # 创建唯一标识：video_itag + audio_itag
                    combination_key = (video_row[2], audio_row[2])  # video_itag, audio_itag
                    if combination_key in written_combinations:
                        auraprint_log(f"【跳过重复组合】{video.video_name} video_itag: {video_row[2]} audio_itag: {audio_row[2]} 在该视频内已存在，跳过",
                                       Config.FINGERPRINT_LOG, "debug")
                        continue
                    written_combinations.add(combination_key)
                    writer.writerow([video_row[0], video_row[1], video_row[2], video_row[3],
                                     video_row[4], audio_row[2], audio_row[3],
                                     audio_row[4], video_row[9], video_row[12],
                                     audio_row[9], audio_row[12]])
                    written_count += 1
            
            # 根据实际写入情况设置flag和日志
            if written_count > 0:
                flag = 'right'
                auraprint_log(f"【新指纹添加】{video.video_name} 添加到指纹库中 (写入 {written_count} 条记录)", 
                               Config.FINGERPRINT_LOG, "info")
            else:
                flag = 'no_data'
                if len(current_category_video) == 0 and len(current_category_audio) == 0:
                    auraprint_log(f"【未添加指纹】{video.video_name} 原因: 无视频和音频itag解析成功", 
                                   Config.FINGERPRINT_LOG, "warning")
                elif len(current_category_video) == 0:
                    auraprint_log(f"【未添加指纹】{video.video_name} 原因: 无视频itag解析成功 (有 {len(current_category_audio)} 个音频itag)", 
                                   Config.FINGERPRINT_LOG, "warning")
                elif len(current_category_audio) == 0:
                    auraprint_log(f"【未添加指纹】{video.video_name} 原因: 无音频itag解析成功 (有 {len(current_category_video)} 个视频itag)", 
                                   Config.FINGERPRINT_LOG, "warning")
                else:
                    auraprint_log(f"【未添加指纹】{video.video_name} 原因: 所有组合都已存在或重复", 
                                   Config.FINGERPRINT_LOG, "warning")
            
            results[video.url] = flag
    
    return results


if __name__ == "__main__":
    # 初始化一个空列表来存储 URL
    url_list = []

    # 1. 从 CSV 读取全部 URL
    try:
        with open(Config.URL_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if row and row[0]:
                    url_list.append(row[0])
    except FileNotFoundError:
        print(f"URL 文件不存在: {Config.URL_FILE}，程序将创建该文件。")
        open(Config.URL_FILE, "w").close()

    # 去重
    url_list = list(set(url_list))
    print(f"从 CSV 读取到 {len(url_list)} 个待采集视频 URL")

    if not url_list:
        print("没有待采集的URL，程序退出。")
        exit(0)

    # 2. 批量创建Video对象
    auraprint_log(f"批量处理开始准备处理 {len(url_list)} 个视频", Config.FINGERPRINT_LOG, "debug")
    video_list = []
    base_timestamp = int(time.time())
    
    for idx, url in enumerate(url_list):
        try:
            # 为每个视频分配唯一的时间戳（避免ID冲突）
            timestamp = base_timestamp + idx
            video = Video(timestamp, url)
            video_list.append(video)
            auraprint_log(f"创建视频对象 {video.video_name} URL: {url}", Config.FINGERPRINT_LOG, "debug")
        except Exception as e:
            auraprint_log(f"创建视频对象失败 URL: {url} 错误: {e}", Config.FINGERPRINT_LOG, "error")
            continue

    if not video_list:
        print("没有成功创建的视频对象，程序退出。")
        exit(0)

    # 3. 批量处理所有视频（下载、分析、写入）
    auraprint_log(f"批量处理开始 {len(video_list)} 个视频", Config.FINGERPRINT_LOG, "debug")
    try:
        results = batch_get_fingerprints(video_list)
        
        # 4. 统计处理结果
        success_count = sum(1 for flag in results.values() if flag == 'right')
        no_data_count = sum(1 for flag in results.values() if flag == 'no_data')
        total_count = len(results)
        
        auraprint_log(f"批量处理完成 总计: {total_count} 个视频, 成功: {success_count} 个, 无数据: {no_data_count} 个", 
                        Config.FINGERPRINT_LOG, "info")
        
        # 输出每个视频的处理结果
        for url, flag in results.items():
            if flag == 'right':
                auraprint_log(f"采集成功 {url}", Config.FINGERPRINT_LOG, "info")
            else:
                auraprint_log(f"采集失败 {url}", Config.FINGERPRINT_LOG, "warning")
                
    except Exception as e:
        auraprint_log(f"批量处理异常：批量处理过程中出错: {e}", Config.FINGERPRINT_LOG, "error")
        import traceback
        auraprint_log(f"批量处理异常详情：{traceback.format_exc()}", Config.FINGERPRINT_LOG, "error")

    print("批量采集完成。")










