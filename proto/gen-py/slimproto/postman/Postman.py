#
# Autogenerated by Thrift Compiler (0.9.3)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:twisted
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
import logging
from ttypes import *
from thrift.Thrift import TProcessor
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TProtocol
try:
  from thrift.protocol import fastbinary
except:
  fastbinary = None

from zope.interface import Interface, implements
from twisted.internet import defer
from thrift.transport import TTwisted

class Iface(Interface):
  def push_notification(token, payload, target_id):
    """
    Parameters:
     - token
     - payload
     - target_id
    """
    pass

  def send_system_mail(receiver, subject, body):
    """
    Parameters:
     - receiver
     - subject
     - body
    """
    pass


class Client:
  implements(Iface)

  def __init__(self, transport, oprot_factory):
    self._transport = transport
    self._oprot_factory = oprot_factory
    self._seqid = 0
    self._reqs = {}

  def push_notification(self, token, payload, target_id):
    """
    Parameters:
     - token
     - payload
     - target_id
    """
    seqid = self._seqid = self._seqid + 1
    self._reqs[seqid] = defer.Deferred()

    d = defer.maybeDeferred(self.send_push_notification, token, payload, target_id)
    d.addCallbacks(
      callback=self.cb_send_push_notification,
      callbackArgs=(seqid,),
      errback=self.eb_send_push_notification,
      errbackArgs=(seqid,))
    return d

  def cb_send_push_notification(self, _, seqid):
    d = self._reqs.pop(seqid)
    d.callback(None)
    return d

  def eb_send_push_notification(self, f, seqid):
    d = self._reqs.pop(seqid)
    d.errback(f)
    return d

  def send_push_notification(self, token, payload, target_id):
    oprot = self._oprot_factory.getProtocol(self._transport)
    oprot.writeMessageBegin('push_notification', TMessageType.ONEWAY, self._seqid)
    args = push_notification_args()
    args.token = token
    args.payload = payload
    args.target_id = target_id
    args.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()
  def send_system_mail(self, receiver, subject, body):
    """
    Parameters:
     - receiver
     - subject
     - body
    """
    seqid = self._seqid = self._seqid + 1
    self._reqs[seqid] = defer.Deferred()

    d = defer.maybeDeferred(self.send_send_system_mail, receiver, subject, body)
    d.addCallbacks(
      callback=self.cb_send_send_system_mail,
      callbackArgs=(seqid,),
      errback=self.eb_send_send_system_mail,
      errbackArgs=(seqid,))
    return d

  def cb_send_send_system_mail(self, _, seqid):
    d = self._reqs.pop(seqid)
    d.callback(None)
    return d

  def eb_send_send_system_mail(self, f, seqid):
    d = self._reqs.pop(seqid)
    d.errback(f)
    return d

  def send_send_system_mail(self, receiver, subject, body):
    oprot = self._oprot_factory.getProtocol(self._transport)
    oprot.writeMessageBegin('send_system_mail', TMessageType.ONEWAY, self._seqid)
    args = send_system_mail_args()
    args.receiver = receiver
    args.subject = subject
    args.body = body
    args.write(oprot)
    oprot.writeMessageEnd()
    oprot.trans.flush()

class Processor(TProcessor):
  implements(Iface)

  def __init__(self, handler):
    self._handler = Iface(handler)
    self._processMap = {}
    self._processMap["push_notification"] = Processor.process_push_notification
    self._processMap["send_system_mail"] = Processor.process_send_system_mail

  def process(self, iprot, oprot):
    (name, type, seqid) = iprot.readMessageBegin()
    if name not in self._processMap:
      iprot.skip(TType.STRUCT)
      iprot.readMessageEnd()
      x = TApplicationException(TApplicationException.UNKNOWN_METHOD, 'Unknown function %s' % (name))
      oprot.writeMessageBegin(name, TMessageType.EXCEPTION, seqid)
      x.write(oprot)
      oprot.writeMessageEnd()
      oprot.trans.flush()
      return defer.succeed(None)
    else:
      return self._processMap[name](self, seqid, iprot, oprot)

  def process_push_notification(self, seqid, iprot, oprot):
    args = push_notification_args()
    args.read(iprot)
    iprot.readMessageEnd()
    d = defer.maybeDeferred(self._handler.push_notification, args.token, args.payload, args.target_id)
    return d

  def process_send_system_mail(self, seqid, iprot, oprot):
    args = send_system_mail_args()
    args.read(iprot)
    iprot.readMessageEnd()
    d = defer.maybeDeferred(self._handler.send_system_mail, args.receiver, args.subject, args.body)
    return d


# HELPER FUNCTIONS AND STRUCTURES

class push_notification_args:
  """
  Attributes:
   - token
   - payload
   - target_id
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'token', None, None, ), # 1
    (2, TType.STRUCT, 'payload', (aps_payload, aps_payload.thrift_spec), None, ), # 2
    (3, TType.I64, 'target_id', None, None, ), # 3
  )

  def __init__(self, token=None, payload=None, target_id=None,):
    self.token = token
    self.payload = payload
    self.target_id = target_id

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.token = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRUCT:
          self.payload = aps_payload()
          self.payload.read(iprot)
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.I64:
          self.target_id = iprot.readI64()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('push_notification_args')
    if self.token is not None:
      oprot.writeFieldBegin('token', TType.STRING, 1)
      oprot.writeString(self.token)
      oprot.writeFieldEnd()
    if self.payload is not None:
      oprot.writeFieldBegin('payload', TType.STRUCT, 2)
      self.payload.write(oprot)
      oprot.writeFieldEnd()
    if self.target_id is not None:
      oprot.writeFieldBegin('target_id', TType.I64, 3)
      oprot.writeI64(self.target_id)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.token)
    value = (value * 31) ^ hash(self.payload)
    value = (value * 31) ^ hash(self.target_id)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)

class send_system_mail_args:
  """
  Attributes:
   - receiver
   - subject
   - body
  """

  thrift_spec = (
    None, # 0
    (1, TType.STRING, 'receiver', None, None, ), # 1
    (2, TType.STRING, 'subject', None, None, ), # 2
    (3, TType.STRING, 'body', None, None, ), # 3
  )

  def __init__(self, receiver=None, subject=None, body=None,):
    self.receiver = receiver
    self.subject = subject
    self.body = body

  def read(self, iprot):
    if iprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None and fastbinary is not None:
      fastbinary.decode_binary(self, iprot.trans, (self.__class__, self.thrift_spec))
      return
    iprot.readStructBegin()
    while True:
      (fname, ftype, fid) = iprot.readFieldBegin()
      if ftype == TType.STOP:
        break
      if fid == 1:
        if ftype == TType.STRING:
          self.receiver = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 2:
        if ftype == TType.STRING:
          self.subject = iprot.readString()
        else:
          iprot.skip(ftype)
      elif fid == 3:
        if ftype == TType.STRING:
          self.body = iprot.readString()
        else:
          iprot.skip(ftype)
      else:
        iprot.skip(ftype)
      iprot.readFieldEnd()
    iprot.readStructEnd()

  def write(self, oprot):
    if oprot.__class__ == TBinaryProtocol.TBinaryProtocolAccelerated and self.thrift_spec is not None and fastbinary is not None:
      oprot.trans.write(fastbinary.encode_binary(self, (self.__class__, self.thrift_spec)))
      return
    oprot.writeStructBegin('send_system_mail_args')
    if self.receiver is not None:
      oprot.writeFieldBegin('receiver', TType.STRING, 1)
      oprot.writeString(self.receiver)
      oprot.writeFieldEnd()
    if self.subject is not None:
      oprot.writeFieldBegin('subject', TType.STRING, 2)
      oprot.writeString(self.subject)
      oprot.writeFieldEnd()
    if self.body is not None:
      oprot.writeFieldBegin('body', TType.STRING, 3)
      oprot.writeString(self.body)
      oprot.writeFieldEnd()
    oprot.writeFieldStop()
    oprot.writeStructEnd()

  def validate(self):
    return


  def __hash__(self):
    value = 17
    value = (value * 31) ^ hash(self.receiver)
    value = (value * 31) ^ hash(self.subject)
    value = (value * 31) ^ hash(self.body)
    return value

  def __repr__(self):
    L = ['%s=%r' % (key, value)
      for key, value in self.__dict__.iteritems()]
    return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

  def __eq__(self, other):
    return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

  def __ne__(self, other):
    return not (self == other)