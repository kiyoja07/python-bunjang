import traceback

import pandas as pd

from src.buntalk.sender import send_buntalk
from src.utils import dfutils
from src.utils.cached_property import cached_property


class BuntalkBulkSender(object):
    BUNTALK_MSG_TYPE_IMAGE = '11'

    def __init__(self, df):
        self._df = df
        self._clean_data()

    @cached_property
    def uids(self):
        return [uid for uid in self._df['target_uid']]

    @classmethod
    def from_csv(cls, path):
        df = pd.read_csv(path)

        return cls(df)

    def _clean_data(self):
        dfutils.parse_number_or_drop(self._df, ['sender_uid', 'target_uid'])
        dfutils.drop_na(self._df, ['message'])
        dfutils.parse_string(self._df, ['message'])
        dfutils.drop_duplicates(self._df, ['target_uid'])
        self._df.drop(
            self._df[
                (
                    ~self._df['image'].isnull()
                    & self._df['thumbnail'].isnull()
                )
                | (
                    self._df['image'].isnull()
                    & ~self._df['thumbnail'].isnull()
                )
            ].index,
            inplace=True,
        )
        dfutils.fill_na(self._df, ['image', 'thumbnail'], '')

    def _send_buntalk_image(self, sender_uid, target_uid, thumbnail, image):
        if not thumbnail or not image:
            return

        send_buntalk(
            str(sender_uid),
            target_uid,
            '',
            msg_type_code=self.BUNTALK_MSG_TYPE_IMAGE,
            extras={
                'sender': {'desc': ''},
                'receiver': {'desc': ''},
                'thumbnailUrl': thumbnail,
                'url': image,
            },
        )

    def _send_buntalk_message(self, sender_uid, target_uid, message):
        send_buntalk(str(sender_uid), target_uid, message)

    def send(self):
        print('uid\timage\tmessage')
        for _, row in self._df.iterrows():
            sender_uid = row['sender_uid']
            target_uid = row['target_uid']
            res_img = False
            res_msg = False

            try:
                self._send_buntalk_image(
                    sender_uid, target_uid, row['thumbnail'], row['image'],
                )
                res_img = True
                self._send_buntalk_message(
                    sender_uid, target_uid, row['message'],
                )
                res_msg = True
            except Exception as e:
                print(e)
                traceback.print_stack(limit=10)
            print('%s\t%s\t%s' % (target_uid, res_img, res_msg))
