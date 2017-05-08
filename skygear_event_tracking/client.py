from queue import Queue, Full, Empty
from requests import Session
from threading import Thread
from urllib.parse import urljoin
import atexit
import datetime
import json
import logging


class Client(object):
    def __init__(
        self,
        skygear_endpoint,
        mount_path='/skygear_event_tracking',
        max_queue_size=1000,
        upload_size=100,
        upload=True,
    ):
        self._log = logging.getLogger('skygear_event_tracking.Client')
        self._endpoint = urljoin(skygear_endpoint, mount_path)
        self._worker = Thread(
            target=self._run_indefinitely,
            daemon=True
        )
        self._upload_size = upload_size
        self._queue = Queue(max_queue_size)
        self._session = Session()
        self._session.headers.update({
            'Content-Type': 'application/json',
        })
        if upload:
            atexit.register(self._cleanup)
            self._running = True
            self._worker.start()
        else:
            self._running = False

    def _sanitize_attributes(self, attributes):
        if attributes is None:
            return None
        output = {}
        for key in attributes:
            if not isinstance(key, str):
                continue
            value = attributes[key]
            if isinstance(value, str):
                output[key] = value
            elif isinstance(value, bool):
                output[key] = value
            elif isinstance(value, int):
                output[key] = float(value)
            elif isinstance(value, float):
                output[key] = value
        return output

    def _serialize_event(self, event):
        output = {}
        for key in event:
            value = event[key]
            if isinstance(value, str):
                output[key] = value
            elif isinstance(value, bool):
                output[key] = value
            elif isinstance(value, int):
                output[key] = float(value)
            elif isinstance(value, float):
                output[key] = value
            elif isinstance(value, datetime.datetime):
                output[key] = {
                    '$type': 'date',
                    '$date': value.isoformat() + 'Z',
                }
        return output

    def _prepare_request_body(self, events):
        body = {
            'events': [],
        }
        for event in events:
            body['events'].append(self._serialize_event(event))
        return json.dumps(body, separators=(',', ':'))

    def _enqueue(self, event):
        try:
            self._queue.put(event, block=False)
            return True
        except Full:
            return False

    def _run_indefinitely(self):
        while self._running:
            self._do_work_in_a_single_loop()

    def _do_work_in_a_single_loop(self):
        self._log.debug('start uploading')
        events = self._gather_next_batch()
        if len(events) <= 0:
            return
        try:
            self._http_post(events)
        except Exception:
            self._log.exception('upload error')
        finally:
            for event in events:
                self._queue.task_done()
        self._log.debug('end uploading')

    def _http_post(self, events):
        data = self._prepare_request_body(events)
        response = self._session.post(
            self._endpoint,
            data=data,
            timeout=15,
        )
        response.raise_for_status()

    def _gather_next_batch(self):
        events = []
        while len(events) < self._upload_size:
            try:
                event = self._queue.get(block=True, timeout=1)
                events.append(event)
            except Empty:
                break
        return events

    def _cleanup(self):
        self._running = False
        try:
            self._worker.join
        except RuntimeError:
            pass

    def track(self, event_name, user_id=None, attributes=None):
        if not event_name:
            return

        event = {}
        user_defined_attributes = self._sanitize_attributes(attributes)
        if user_defined_attributes is not None:
            event.update(user_defined_attributes)

        event['_tracked_at'] = datetime.datetime.utcnow()
        event['_event_raw'] = event_name
        if user_id:
            event['_user_id'] = user_id

        return self._enqueue(event)
