from warcio.warcwriter import WARCWriter
from io import BytesIO
import os
from datetime import datetime


class WarcSaver:
    def __init__(self, warc_path):
        os.makedirs(os.path.dirname(warc_path), exist_ok=True)
        self.output = open(warc_path, "ab")
        self.writer = WARCWriter(self.output, gzip=True)

    def write_response(self, url, response):
        if not response.body:
            return  # Skip empty responses

        headers = '\r\n'.join(
            f"{k.decode(errors='ignore')}: {v.decode(errors='ignore')}"
            for k, v in response.headers.items()
        )
        http_headers = f"HTTP/1.1 {response.status} OK\r\n{headers}\r\n\r\n"
        http_payload = BytesIO(http_headers.encode('utf-8') + response.body)

        record = self.writer.create_warc_record(
            url=url,
            record_type="response",
            payload=http_payload,
            warc_headers_dict={
                'WARC-Date': datetime.utcnow().isoformat() + 'Z',
                'WARC-Target-URI': url,
                'Content-Type': 'application/http; msgtype=response'
            }
        )
        self.writer.write_record(record)

    def close(self):
        self.output.close()
