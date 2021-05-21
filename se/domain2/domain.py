import logging
import threading
from configparser import ConfigParser
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL
from typing import Mapping
import threading


class BeanContainer(object):
    beans: Mapping[type, object] = {}

    @classmethod
    def getBean(cls, the_type: type):
        return cls.beans[the_type]

    @classmethod
    def register(cls, the_type: type, bean: object):
        cls.beans[the_type] = bean

def synchronized(func):
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)

    return synced_func


def send_email(title: str, content: str):
    from se import config
    if config.get("email", 'activate') == 'false':
        return

    def send_with_retry():
        retry_limit = 10
        for i in range(retry_limit):
            try:
                do_send_email(title, content, config)
                break
            except:
                import traceback
                logging.warning("发送邮件失败 {},邮件配置:{}".format(traceback.format_exc(), config.__dict__))
                import time
                time.sleep(10)

    threading.Thread(name='send_email', target=send_with_retry).start()


@synchronized
def do_send_email(title: str, content: str, config: ConfigParser):
    # 登录
    logging.info("开始发送邮件")
    smtp = SMTP_SSL(config.get('email', 'host_server'))
    smtp.set_debuglevel(0)
    smtp.ehlo(config.get('email', 'host_server'))
    passwords = config.get('email', 'password').split(",")
    import random
    k = random.randint(0, len(passwords)-1)
    password = passwords[k]
    smtp.login(config.get('email', 'username'), password)
    if 'ib_account' in config.sections():
        name = config.get("ib_account", "name")
    elif 'td_account' in config.sections():
        name = config.get("td_account", "name")
    else:
        raise RuntimeError("config error")
    title = '[{}]{}'.format(name, title)
    sender_email = config.get('email', 'sender_email')
    receiver = config.get('email', 'receiver')
    # 替换掉content中的< >字符
    content = content.replace('<', '[').replace('>', ']')
    msg = MIMEText(content, "plain", 'utf-8')
    msg["Subject"] = Header(title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = receiver
    smtp.sendmail(sender_email, receiver, msg.as_string())
    smtp.quit()
