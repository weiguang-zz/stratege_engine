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
                logging.warning("发送邮件失败 {}".format(traceback.format_exc()))
                import time
                time.sleep(10)

    threading.Thread(name='send_email', target=send_with_retry).start()




@synchronized
def do_send_email(title: str, content: str, config: ConfigParser):
    # 登录
    smtp = SMTP_SSL(config.get('email', 'host_server'))
    smtp.set_debuglevel(0)
    smtp.ehlo(config.get('email', 'host_server'))
    smtp.login(config.get('email', 'username'), config.get('email', 'password'))

    title = '[{}]{}'.format(config.get("ib_account", "name"), title)
    sender_email = config.get('email', 'sender_email')
    receiver = config.get('email', 'receiver')
    msg = MIMEText(content, "plain", 'utf-8')
    msg["Subject"] = Header(title, 'utf-8')
    msg["From"] = sender_email
    msg["To"] = receiver
    smtp.sendmail(sender_email, receiver, msg.as_string())
    smtp.quit()