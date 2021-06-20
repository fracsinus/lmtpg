from datetime import datetime
from smtpd import SMTPChannel, SMTPServer
import traceback
import uuid

import psycopg2


class LMTPChannel(SMTPChannel):
    def smtp_LHLO(self, arg):
        self.smtp_HELO(arg)


class LMTPGServer(SMTPServer):
    channel_class = LMTPChannel

    def __init__(self, localaddr, remoteaddr,
                 pg_host, pg_port, pg_dbname, pg_user, pg_password,
                 **kwargs,):
        SMTPServer.__init__(self, localaddr, remoteaddr, **kwargs)
        try:
            self.pg_conn = psycopg2.connect(
                host=pg_host, port=pg_port, dbname=pg_dbname, user=pg_user,
                password=pg_password
            )
        except Exception as err:
            self.close()
            raise err

    def process_message(self, peer, mailfrom, rcpttos, data, **kwargs):
        now = datetime.now().isoformat(sep=' ')
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    'INSERT INTO email'
                    ' (uuid, created_at, mailfrom, rcptto, length, bytedata)'
                    ' values (%s, %s, %s, %s, %s, %s)',
                    vars=[str(uuid.uuid4()), now, mailfrom,
                          ', '.join(rcpttos), len(data), data]
                )
            self.pg_conn.commit()
        except Exception:
            self.pg_conn.rollback()
            print(
                f'[{now}] DB INSERTION ERROR:\n',
                traceback.format_exc(),
                flush=True
            )
