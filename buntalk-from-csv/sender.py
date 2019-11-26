import sys

from src.buntalk.bulk_sender import BuntalkBulkSender


def _confirm(uids):
    count = len(uids)
    uids = [str(uid) for uid in uids[:5]]
    print('%s' % ', '.join(uids))
    res = input('총 %s 명. 진행합니까? (Y/n): ' % count)
    return res in ['y', 'Y']


def main(fpath):
    sender = BuntalkBulkSender.from_csv(fpath)

    if not _confirm(sender.uids):
        print('취소 되었습니다.')
        return

    sender.send()
    print('Done.')


if __name__ == '__main__':
    """
    Usage:
        python sender.py example.csv
    To skip interaction:
        echo y | nohup python -u sender.py example.csv &
    """
    if len(sys.argv) < 2:
        print('Usage: %s <file>' % sys.argv[0])
        sys.exit(1)

    main(sys.argv[1])
