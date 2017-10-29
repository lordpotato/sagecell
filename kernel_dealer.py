import time

import jupyter_client.session
import zmq

from log import logger
import misc


config = misc.Config()


class Kernel(object):
    """
    Kernel from the dealer point of view.
    
    Manages connections over ZMQ sockets to compute kernels.
    """
    
    def __init__(self, dealer, id, connection, timeout):
        self._dealer = dealer
        self.id = id
        self.connection = connection
        self.executing = 0
        now = time.time()
        self.hard_deadline = now + config.get("max_lifespan")
        self.timeout = min(timeout, config.get("max_timeout"))
        self.deadline = min(now + self.timeout, self.hard_deadline)
        conn = self.connection
        self.session = jupyter_client.session.Session(key=conn["key"])
        self.channels = {}
        context = zmq.Context.instance()
        address = conn["ip"]
        if ":" in address:
            address = "[{}]".format(address)
        for channel, socket_type in (
                ("shell", zmq.DEALER), ("iopub", zmq.SUB), ("hb", zmq.REQ)):
            socket = context.socket(socket_type)
            if ":" in address: # FIXME - do we need it conditionally?
                socket.setsockopt(zmq.IPV6, 1)
            socket.connect("tcp://{}:{}".format(address, conn[channel]))
            stream = zmq.eventloop.zmqstream.ZMQStream(socket)
            stream.channel = channel
            self.channels[channel] = stream
        self.channels["iopub"].socket.setsockopt(zmq.SUBSCRIBE, b"")
        self.start_hb()
        logger.debug("kernel initialized")
        
    def start_hb(self):
        logger.debug("start_hb for %s", self.id)
        self._alive = True
        hb_stream = self.channels["hb"]

        def beat_received(message):
            #logger.debug("beat_received for %s", self.id)
            self._alive = True

        hb_stream.on_recv(beat_received)

        def ping_or_dead():
            #logger.debug("ping_or_dead for %s", self.id)
            hb_stream.flush()
            now = time.time()
            if now > self.deadline and self.executing == 0:
                # only kill the kernel after all pending
                # execute requests have finished
                self._alive = False
            if now > self.hard_deadline:
                self._alive = False
                logger.info("hard deadline reached for %s", self.id)
            if self._alive:
                self._alive = False
                hb_stream.send(b'ping')
                # flush stream to force immediate socket send
                hb_stream.flush()
            else:
                self.kernel_died()
                self.stop_hb()

        loop = zmq.eventloop.IOLoop.instance()
        self._hb_periodic_callback = zmq.eventloop.ioloop.PeriodicCallback(
            ping_or_dead, config.get("beat_interval") * 1000, loop)

        def delayed_start():
            # Make sure we haven't been closed during the wait.
            logger.debug("delayed_start for %s", self.id)
            if self._beating and not hb_stream.closed():
                self._hb_periodic_callback.start()

        self._start_hb_handle = loop.add_timeout(
            time.time() + config.get("first_beat"), delayed_start)
        self._beating = True

    def stop(self):
        logger.debug("stopping kernel %s", self.id)
        self.stop_hb()
        for stream in self.channels.itervalues():
            stream.close()
        self._dealer.stop_kernel(self.id)
        
    def stop_hb(self):
        logger.debug("stop_hb for %s", self.id)
        if not self._beating:
            logger.error("heart was not beating")
            return
        self._beating = False
        self._hb_periodic_callback.stop()
        zmq.eventloop.IOLoop.instance().remove_timeout(self._start_hb_handle)
        self.channels["hb"].on_recv(None)


class KernelDealer(object):
    r"""
    Kernel Dealer handles compute kernels on the server side.
    """
    
    def __init__(self, provider_settings):
        self.provider_settings = provider_settings
        self._available_providers = []
        self._connected_providers = set()
        self._expected_kernels = []
        self._get_queue = []
        self._kernel_origins = dict()   # id: provider address
        # id: {
        #   "connection": { ??????????????????????????????????????????
        #     "key": session key,
        #     "hb": hb port,
        #     "iopub": iopub port,
        #     "shell": shell port,
        #   },
        #   "executing": integer,
        #   "timeout": time,
        #   "hard_deadline": time,
        #   "deadline": time,
        # }        
        context = zmq.Context.instance()
        socket = context.socket(zmq.ROUTER)
        socket.setsockopt(zmq.IPV6, 1)
        self.port = socket.bind_to_random_port("tcp://*")
        # Can configure perhaps interface/IP/port
        self._stream = zmq.eventloop.zmqstream.ZMQStream(socket)
        self._stream.on_recv(self._recv)
        logger.debug("KernelDealer initialized")
        
    def _try_to_get(self):
        r"""
        Send a get request if possible AND needed.
        """
        while self._available_providers and self._get_queue:
            self._stream.send(self._available_providers.pop(0), zmq.SNDMORE)
            self._stream.send_json(["get", self._get_queue.pop(0)])
        if self._available_providers:
            logger.debug("%s available providers are idling",
                len(self._available_providers))
        if self._get_queue:
            logger.debug("%s get requests are waiting for providers",
                len(self._get_queue))
        
    def _recv(self, msg):
        logger.debug("received %s", msg)
        assert len(msg) == 2
        addr = msg[0]
        self._connected_providers.add(addr)
        msg = zmq.utils.jsonapi.loads(msg[1])
        if msg == "get settings":
            self._stream.send(addr, zmq.SNDMORE)
            self._stream.send_json(["settings", self.provider_settings])
        elif msg == "ready":
            self._available_providers.append(addr)
            self._try_to_get()
        elif msg[0] == "kernel":
            msg = msg[1]
            for i, (limits, callback) in enumerate(self._expected_kernels):
                if limits == msg["limits"]:
                    self._kernel_origins[msg["id"]] = addr
                    self._expected_kernels.pop(i)
                    callback(msg)                    
            
    def get_kernel(self, resource_limits, timeout, callback):

        def cb(d):
            d.pop("limits")
            d["timeout"] = timeout
            kernel = Kernel(self, **d)
            logger.info("dealing kernel %s", kernel.id)
            callback(kernel)
            
        self._expected_kernels.append((resource_limits, cb))
        self._get_queue.append(resource_limits)
        self._try_to_get()
        
    def stop(self):
        r"""
        Notify all providers that we are shutting down.
        """
        self._stream.stop_on_recv()
        for addr in self._connected_providers:
            logger.debug("stopping %r", addr)
            self._stream.send(addr, zmq.SNDMORE)
            self._stream.send_json("disconnect")
        self._stream.flush()

    def stop_kernel(self, id):
        addr = self._kernel_origins.pop(id)
        self._stream.send(addr, zmq.SNDMORE)
        self._stream.send_json(["stop", id])
