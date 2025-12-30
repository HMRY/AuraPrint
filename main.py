# 支持视频+音频指纹，支持获取时间线
import sys, os, json, re, subprocess, numpy as np, datetime, requests, time, csv
from bs4 import BeautifulSoup
from itertools import accumulate
from concurrent.futures import ThreadPoolExecutor

from video_config import Config, log_match_result

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
            log_match_result(
                f"【解析MP4格式元数据未下载该视频】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {video_name} {str(self.itag)} 文件不存在",
                Config.FINGERPRINT_LOG, "error")
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
            log_match_result(
                f"【解析MP4格式元数据版本错误】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {self.username} {video_name} {str(self.itag)} {str(self.Version)}",
                Config.FINGERPRINT_LOG, "error")
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
            log_match_result(
                f"【解析WebM格式元数据未下载该视频】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {self.username} {video_name} {str(self.itag)} ",
                Config.FINGERPRINT_LOG, "error")
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
        # log_match_result(f"【开始get websource】 {self.url} ",Config.FINGERPRINT_LOG,"debug")
        if not os.path.exists(self.down_path + r'websource/'):
            os.makedirs(self.down_path + r'websource/')
        response_path = self.down_path + r'websource/' + self.video_name + '.html'
        # 避免下载失败/重复下载
        while not os.path.exists(response_path):
            # 方案1
            # log_match_result(f"【开始下载 websource】 {self.url} ",Config.FINGERPRINT_LOG,"debug")
            # response = requests.get(self.url)#请求
            # if response.status_code == 200:
            #     with open(self.down_path + r'websource/' + self.video_name + '.html', 'w', encoding='utf-8') as f:
            #         f.write(response.text)
            try:
                # 开始记录请求时间
                start_time = time.time()
                log_match_result(f"【开始下载 websource】 {self.url} ", Config.FINGERPRINT_LOG, "debug")

                # 设置超时时间
                response = requests.get(self.url, Config.WEBSOURCE_TIMEOUT)

                # 检查请求状态
                if response.status_code == 200:
                    elapsed_time = time.time() - start_time  # 计算请求耗时
                    log_match_result(f"【websource下载完成】耗时 {elapsed_time:.2f} 秒 {self.url}",
                                     Config.FINGERPRINT_LOG, "info")

                    # 保存下载的网页内容
                    with open(self.down_path + r'websource/' + self.video_name + '.html', 'w', encoding='utf-8') as f:
                        f.write(response.text)
                else:
                    log_match_result(f"【请求失败】状态码: {response.status_code} URL: {self.url}",
                                     Config.FINGERPRINT_LOG, "error")

            except requests.exceptions.Timeout:
                log_match_result(f"【请求超时】超过30秒未响应 URL: {self.url}", Config.FINGERPRINT_LOG, "error")
            except requests.exceptions.RequestException as e:
                log_match_result(f"【请求异常】URL: {self.url} 错误: {e}", Config.FINGERPRINT_LOG, "error")

            # # 方案2
            # print(f"\nvideo={self.video_name} Websource is downloading...")
            # # 因为存在异步加载，所以需要用driver来模拟请求网页源码
            # # 选项配置
            # options = webdriver.ChromeOptions()

            # # 无头模式，不打开实际浏览器窗口
            # options.add_argument("--headless")
            # options.add_argument("--disable-logging")

            # # 创建 Chrome 浏览器实例
            # driver = webdriver.Chrome(options=options)
            # driver.get(self.url)  # 请求
            # with open(self.down_path + r'websource/' + self.video_name + '.html', 'w', encoding='utf-8') as f:
            #     f.write(driver.page_source)
            # driver.quit()
        log_match_result(f"【Websource 已存在】Video name： {self.video_name} ", Config.FINGERPRINT_LOG, "debug")

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
        self.itag_quality = {}
        for param in service_tracking_params:
            itag = param.get('itag')
            is_drc = param.get('isDrc')  # None/True
            if is_drc:
                itag = str(itag) + "-drc"

            # print("【itag】",itag)

            if itag in (self.video_mp4_itag + self.video_webm_itag):
                indexRange = param.get('indexRange', {'start': 0, 'end': 0})  # modify
                if indexRange == {'start': 0, 'end': 0}:
                    log_match_result(
                        f"【解析websourse该itag没有indexRange】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {self.username} {self.url} {str(itag)} ",
                        Config.FINGERPRINT_LOG, "error")
                else:
                    self.itag_list.append(itag)  # 只下载有IndexRange的

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
                    log_match_result(
                        f"【解析websourse该itag不合法的indexRange】{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {self.username} {self.url} {str(itag)} ",
                        Config.FINGERPRINT_LOG, "error")
                else:
                    self.itag_list.append(itag)  # 只下载有IndexRange的

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
        # log_match_result(f"【正在解析指纹】",Config.FINGERPRINT_LOG,"debug")

        for itag in self.itag_list:
            start, end = self.itag_indexrange[itag]['start'], self.itag_indexrange[itag]['end']
            ##################获取一个itag的视频指纹##################
            box = Box(itag, start, end, self.video_name, self.down_path)
            quality = self.itag_quality[itag]
            vcodec = self.itag_vcodec[itag]
            contentLength = self.itag_contentlength[itag]
            self.itag_box[itag] = box
            if hasattr(box, 'reference_list'):
                duration_list = [1000 * x // box.Timescale for x in box.duration_list]
                timeline = [0] + list(accumulate(duration_list))
                condition = (contentLength == end + 1 + sum(box.reference_list))
                log_match_result(
                    f"【正在解析指纹】{itag:<9}, {self.itag_contentlength[itag]:<12}, 'fmp4', {end + 1 + sum(box.reference_list):<12}, {(condition if condition == False else ''):<8}, {contentLength - (end + 1 + sum(box.reference_list))}",
                    Config.FINGERPRINT_LOG, "debug")
                if itag != 140 or (itag == 140 and condition):
                    fingerprint_list.append(
                        [self.ID, self.url, itag, quality, 'fmp4', vcodec, start, end, contentLength,
                         '/'.join(map(str, box.reference_list)), box.Timescale,
                         '/'.join(map(str, box.duration_list)), '/'.join(map(str, timeline))])
            else:
                duration_list = (np.diff(box.timeline)).tolist()
                fingerprint_list.append([self.ID, self.url, itag, quality, 'webm', vcodec, start, end, contentLength,
                                         '/'.join(map(str, box.track_list)), '1000', '/'.join(map(str, duration_list)),
                                         '/'.join(map(str, box.timeline))])
                log_match_result(
                    f"【正在解析指纹】{itag:<9}, {self.itag_contentlength[itag]:<12}, 'webm', {end + 1 + sum(box.track_list):<12}, {(contentLength == end + 1 + sum(box.track_list)):<8}, {contentLength - (end + 1 + sum(box.track_list))}",
                    Config.FINGERPRINT_LOG, "debug")
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
        log_match_result(f"【开始下载】itag={itag}", Config.FINGERPRINT_LOG, "debug")
        
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
            log_match_result(f"【下载失败】itag={itag}：下载超时。下载超时（{ITAG_DL_TIMEOUT}秒），进程被终止。",
                             Config.FINGERPRINT_LOG, "error")
        except Exception as e:
            log_match_result(f"【下载失败】itag={itag} 错误：{e}", Config.FINGERPRINT_LOG, "error")

        # 检查下载是否成功，失败/小于MIN_ITAG_DL_SIZE则删掉
        if (os.path.exists(videopath + '.part') and os.path.getsize(videopath + '.part') >= MIN_ITAG_DL_SIZE) or \
                (os.path.exists(videopath) and os.path.getsize(videopath) >= MIN_ITAG_DL_SIZE):
            log_match_result(f"【下载完成】{videopath}(.part)", Config.FINGERPRINT_LOG, "info")
            return videopath
        else:
            # 如果文件未下载完成或文件小于规定的最小大小，删除文件
            for path in [videopath + '.part', videopath]:
                if os.path.exists(path):
                    os.remove(path)  # 删除文件
                    log_match_result(f"【删除文件】{videopath}(.part) 文件大小小于 {MIN_ITAG_DL_SIZE} 字节",
                                     Config.FINGERPRINT_LOG, "debug")

            # 如果文件未成功下载，记录下载失败的详细信息
            if os.path.exists(videopath + '.part'):
                log_match_result(f"【下载失败】{videopath}(.part) 文件存在，但未成功下载或被中断", Config.FINGERPRINT_LOG,
                                 "error")
            elif os.path.exists(videopath):
                log_match_result(f"【下载失败】{videopath} 文件已存在，但大小小于 {MIN_ITAG_DL_SIZE} 字节",
                                 Config.FINGERPRINT_LOG, "error")
            else:
                log_match_result(f"【下载失败】{videopath} 未下载或文件不存在", Config.FINGERPRINT_LOG, "error")

            return None


def process_videos(video_list, MAX_THREADS, ITAG_DL_TIMEOUT, MIN_ITAG_DL_SIZE, MAX_RETRIES=3):
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
                    log_match_result(f"【跳过重试】视频 {video.url} 已达到最大重试次数({MAX_RETRIES}次)", 
                                   Config.FINGERPRINT_LOG, "warning")
                    continue
                
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
                        print(f"{itag}.part exists.")
                        log_match_result(f"【下载跳过】{itag}.part 已存在.", Config.FINGERPRINT_LOG, "debug")

                        continue  # good文件已存在，跳过下载任务

                    tasks.append((video, executor.submit(video.download_video, itag, ITAG_DL_TIMEOUT, MIN_ITAG_DL_SIZE)))  # 提交任务
                    # print(f"add {itag}.")
                    log_match_result(f"【增加itag】{itag}", Config.FINGERPRINT_LOG, "debug")

            # 等待所有线程完成
            for video, task in tasks:
                try:
                    videopath = task.result()  # 会等待task结
                    if videopath is None:
                        # 记录失败，准备重试
                        if video not in current_retries:
                            current_retries[video] = True
                        log_match_result(f"【下载失败】视频 {video.url} 的某个itag下载失败，将重试", 
                                       Config.FINGERPRINT_LOG, "debug")

                except Exception as e:
                    # 记录失败，准备重试
                    if video not in current_retries:
                        current_retries[video] = True
                    log_match_result(f"【下载失败】视频 {video.url} 下载异常: {e}，将重试", 
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
                log_match_result(f"【准备重试】视频 {video.url} 第 {retries[video]} 次重试", 
                               Config.FINGERPRINT_LOG, "info")
            else:
                log_match_result(f"【下载最终失败】视频 {video.url} 已达到最大重试次数({MAX_RETRIES}次)，停止重试", 
                               Config.FINGERPRINT_LOG, "error")
        
        # 更新video_list为需要重试的视频
        video_list = next_retry_list
        
        if video_list:
            log_match_result(f"【开始下一轮重试】剩余 {len(video_list)} 个视频需要重试", 
                           Config.FINGERPRINT_LOG, "info")

    # 统计最终结果
    failed_count = sum(1 for count in retries.values() if count > MAX_RETRIES)
    if failed_count > 0:
        log_match_result(f"【部分下载失败】有 {failed_count} 个视频下载失败，已达到最大重试次数({MAX_RETRIES}次)", 
                       Config.FINGERPRINT_LOG, "warning")
    else:
        log_match_result(f"【全部下载完成】", Config.FINGERPRINT_LOG, "info")


def online_get_fingerprint(url, timestamp):
    flag = 0
    video = Video(timestamp, url)  # 创建一个视频对象，索引为0
    video_list = [video]  # 视频对象列表，包含一个视频对象

    process_videos(video_list, Config.MAX_THREADS, Config.ITAG_DL_TIMEOUT, Config.MIN_ITAG_DL_SIZE, Config.MAX_RETRIES)  # 线程控制

    for video in video_list:
        fingerprint_list = video.analyse_video()  # 分析文件

    log_match_result(f"【指纹采集完成】", Config.FINGERPRINT_LOG, "info")

    # # 输出 fingerprint_list 列表
    # print("fingerprint_list:")
    # for fingerprint in fingerprint_list:
    #     print(fingerprint)

    # 指纹混合
    pattern = r'^\d+x\d+$'  # 定义一个正则表达式，匹配视频质量的格式，如1280x720
    current_category_video = []
    current_category_audio = []
    for row in fingerprint_list:
        if re.match(pattern, row[3]):  # 假设视频质量在第4列
            current_category_video.append(row)  # 如果是视频质量，初始化视频指纹数据
        else:
            current_category_audio.append(row)  # 如果不是视频质量，初始化音频指纹数据

    # # 输出 current_category_video 列表
    # print("Video List:")
    # for video in current_category_video:
    #     print(video)

    # # 输出 current_category_audio 列表
    # print("\nAudio List:")
    # for audio in current_category_audio:
    #     print(audio)

    # 写入指纹库
    # 检查文件是否存在，如果不存在或为空则写入表头
    file_exists = os.path.exists(Config.FINGERPRINT_FILE)
    write_header = False
    if not file_exists or os.path.getsize(Config.FINGERPRINT_FILE) == 0:
        write_header = True
    
    with open(Config.FINGERPRINT_FILE, 'a', newline='', encoding='utf-8') as processed_file:
        writer = csv.writer(processed_file)
        # 如果需要写入表头，先写入表头
        if write_header:
            header = ['ID', 'url', 'video_itag', 'video_quality', 'video_format', 
                     'audio_itag', 'audio_quality', 'audio_format', 
                     'video_fp', 'video_timeline', 'audio_fp', 'audio_timeline']
            writer.writerow(header)
        
        for video_row in current_category_video:
            for audio_row in current_category_audio:
                writer.writerow([video_row[0], video_row[1], video_row[2], video_row[3],
                                 video_row[4], audio_row[2], audio_row[3],
                                 audio_row[4], video_row[9], video_row[12],
                                 audio_row[9], audio_row[12]])
        flag = 'right'

    log_match_result(f"新 指纹 添加到 指纹库中: {timestamp}-{url}", Config.FINGERPRINT_LOG, "info")

    return flag


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
    # url_list = list(set(url_list))
    print(f"从 CSV 读取到 {len(url_list)} 个 URL")

    # 2. 按序批量采集
    for url in url_list:
        log_match_result(f"采集开始: {url}", Config.FINGERPRINT_LOG, "info")

        try:
            timestamp = int(time.time())
            flag = online_get_fingerprint(url, timestamp)
        except Exception as e:
            log_match_result(f"采集指纹时出错: {e}", Config.FINGERPRINT_LOG, "error")
            continue

        if flag == 'right':
            log_match_result(f"采集成功: {url}", Config.FINGERPRINT_LOG, "info")
        else:
            log_match_result(f"采集失败: {url}", Config.FINGERPRINT_LOG, "warning")

    print("批量采集流程完成。")










