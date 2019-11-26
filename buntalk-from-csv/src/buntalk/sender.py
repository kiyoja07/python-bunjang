import json
import requests
from .config import BUNTALK_HOST


def send_buntalk(sender_uid, target_uid, msg, msg_type_code='1', extras=None):
    if extras is None:
        extras = {}

    s = requests.Session()
    a = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount('http://', a)

    # 채널 가져옴
    headers = {'LOGIN_UID': sender_uid, 'LOGIN_TOKEN': ''}
    join_chat_api_url = '{0}/chat/join.json'.format(BUNTALK_HOST)
    params = dict(uids=target_uid)
    rw = s.get(join_chat_api_url, headers=headers, params=params).json()
    channelId = rw['result']['channelId']

    # 메시지 보내기
    send_chat_api_url = '{0}/hawaii/buntalk/send/message'.format(BUNTALK_HOST)
    data = dict()
    data['tid'] = ''
    data['channel_id'] = channelId
    data['extras'] = json.dumps(extras)
    data['msg'] = msg
    data['msg_type_code'] = msg_type_code
    data['writer_uid'] = sender_uid
    data['target_uid'] = target_uid

    rs = s.post(
        send_chat_api_url,
        data=data,
        headers=dict(),
        params=dict(),
        timeout=1000
    ).json()
    return rs.get('status')
