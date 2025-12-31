import logging
import os
from datetime import datetime

# 配置类
class Config:
    # 输入：URL文件
    URL_FILE = r'./data/url_test.csv'    
    # 输出：指纹库文件
    FINGERPRINT_FILE = r'./data/fingerprint_test.csv'

    # 指纹下载路径
    FINGERPRINT_DOWN = './data/fingerprint_download/'
    # 采指纹日志文件
    FINGERPRINT_LOG = './log/'   
    # 日志级别 ：DEBUG/INFO/WARNING/ERROR
    LOG_LEVEL = logging.DEBUG

    #采集指纹参数配置
    MAX_THREADS = 1
    WEBSOURCE_TIMEOUT = 5 #websource下载超时时间，单位为秒
    WEBSOURCE_MAX_RETRIES = 1  # websource下载最大次数（包含重试）
    ITAG_DL_TIMEOUT = 5  # 单个itag下载超时时间，单位为秒
    MAX_RETRIES = 1  # itag下载最大重试次数
    MIN_ITAG_DL_SIZE = 20480  # 单个itag下载最小文件大小，单位为字节
 


# 记录匹配日志到txt文件
def log_match_result(data, dir, level='INFO', log_name="VIDEO_LOG"):
    """
    记录日志消息到文件
    :param data: str, 日志内容
    :param level: str, 日志级别（DEBUG、INFO、WARNING、ERROR）
    :param log_name: str, 日志记录器名称
    """
    # 动态生成日志文件名，基于当前日期
    # 获取当前日期
    current_date = datetime.now().strftime('%Y-%m-%d')

    # 确保日志目录存在
    if not os.path.exists(dir):
        os.makedirs(dir)
    
    # 创建日志文件路径
    log_file = os.path.join(dir, f"{current_date}.log")

    # 配置日志记录器
    logger = logging.getLogger(log_name)

    # 检查日志记录器是否已有处理器，且处理器的目标文件是否为当天的日志文件
    if logger.hasHandlers():
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                if handler.baseFilename != os.path.abspath(log_file):
                    logger.removeHandler(handler)
    
    if not logger.hasHandlers():  # 防止重复添加处理器
        logger.setLevel(Config.LOG_LEVEL)  # 设置最低日志级别
        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        # 设置日志格式
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%a %b %d %H:%M:%S %Y'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # === 控制台处理器（新增） ===
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 根据级别记录日志
    level = level.upper()  # 转为大写以匹配标准日志级别
    if level == 'DEBUG':
        logger.debug(data)
    elif level == 'INFO':
        logger.info(data)
    elif level == 'WARNING':
        logger.warning(data)
    elif level == 'ERROR':
        logger.error(data)
    else:
        logger.error(f"Invalid log level: {level}. Message: {data}")