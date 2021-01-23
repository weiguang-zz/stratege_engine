import logging
from email.header import Header
from email.mime.text import MIMEText
from smtplib import SMTP_SSL
from typing import Mapping
from se.domain2 import config

class BeanContainer(object):

    beans: Mapping[type, object] = {}

    @classmethod
    def getBean(cls, the_type: type):
        return cls.beans[the_type]

    @classmethod
    def register(cls, the_type: type, bean: object):
        cls.beans[the_type] = bean


def send_email(title: str, content: str):
    try:
        # 登录
        smtp = SMTP_SSL(config.get('email', 'host_server'))
        smtp.set_debuglevel(0)
        smtp.ehlo(config.get('email', 'host_server'))
        smtp.login(config.get('email', 'username'), config.get('email', 'password'))

        sender_email = config.get('email', 'sender_email')
        receiver = config.get('email', 'receiver')
        msg = MIMEText(content, "plain", 'utf-8')
        msg["Subject"] = Header(title, 'utf-8')
        msg["From"] = sender_email
        msg["To"] = receiver
        smtp.sendmail(sender_email, receiver, msg.as_string())
        smtp.quit()
    except:
        import traceback
        logging.error("{}".format(traceback.format_exc()))
