#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
from io import BytesIO

import requests
from requests import exceptions as request_exceptions
from thrift.transport.TTransport import TTransportBase, TTransportException
from twitter.common import log

try:
  from urlparse import urlparse
except ImportError:
  from urllib.parse import urlparse


DEFAULT_USER_AGENT = 'Python TRequestsTransport v1.0'


def default_requests_session_factory():
  session = requests.session()
  return session


class TRequestsTransport(TTransportBase):
  """A Thrift HTTP client based upon the requests module."""

  def __init__(
      self,
      uri,
      auth=None,
      session_factory=default_requests_session_factory,
      user_agent=DEFAULT_USER_AGENT):

    """Construct a TRequestsTransport.

    Construct a Thrift transport based upon the requests module.  URI is the
    HTTP endpoint that the server should be listening on.  The 'auth'
    keyword is passed directly to the requests client and can be used to
    provide different authentication contexts such as Kerberos
    authentication via the requests-kerberos module.

    :param uri: The endpoint uri
    :type uri: str
    :keyword auth: The requests authentication context.
    :keyword session_factory: A callable that returns a requests session.
    :keyword user_agent: The value to use for the User-Agent header.
    """
    self._session = None
    self.__session_factory = session_factory
    if not callable(session_factory):
      raise TypeError('session_factory should be a callable that produces a requests.Session!')
    self.__user_agent = user_agent
    self.__wbuf = BytesIO()
    self.__rbuf = BytesIO()
    self.__uri = uri
    try:
      self.__urlparse = urlparse(uri)
    except ValueError:
      raise TTransportException('Failed to parse uri %r' % (uri,))
    self.__timeout = None
    self.__auth = auth

    # Silence requests logs so we don't get messages for every HTTP connection.
    logging.getLogger('requests').setLevel(logging.WARNING)

  def isOpen(self):
    return self._session is not None

  def open(self):
    session = self.__session_factory()
    requests_default_agent = requests.utils.default_user_agent()
    if session.headers.get('User-Agent', requests_default_agent) == requests_default_agent:
      session.headers['User-Agent'] = self.__user_agent

    self._session = session

  def close(self):
    session, self._session = self._session, None
    session.close()

  def setTimeout(self, ms):
    self.__timeout = ms / 1000.0

  def read(self, size):
    return self.__rbuf.read(size)

  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    if self.isOpen():
      self.close()

    self.open()

    data = self.__wbuf.getvalue()
    self.__wbuf = BytesIO()

    self._session.headers['Content-Type'] = 'application/x-thrift'
    self._session.headers['Content-Length'] = str(len(data))
    self._session.headers['Host'] = self.__urlparse.hostname

    response = None
    try:
      response = self._session.post(
          self.__uri,
          data=data,
          timeout=self.__timeout,
          auth=self.__auth)
      response.raise_for_status()
    except request_exceptions.Timeout:
      raise TTransportException(
          type=TTransportException.TIMED_OUT,
          message='Timed out talking to %s' % self.__uri)
    except request_exceptions.RequestException as e:
      if response:
        log.debug('Error connecting, logging response headers:.')
        for field_name, field_value in response.headers.items():
          log.debug('  %s: %s' % (field_name, field_value))
      raise TTransportException(
          type=TTransportException.UNKNOWN,
          message='Unknown error talking to %s: %s' % (self.__uri, e))

    self.__rbuf = BytesIO(response.content)
