#! /usr/bin/env python

r"""
Kernel Provider starts compute kernels and sends connection info to Dealer.
"""


import argparse
import errno
from multiprocessing import Process
import os
import resource
import signal
import sys
import uuid

from ipykernel.kernelapp import IPKernelApp
import zmq

import kernel_init
import log
logger = log.provider_logger.getChild(str(os.getpid()))


class Kernel(Process):
    """
    Kernel from the provider point of view.
    
    Configures a kernel process and does its best at cleaning up.
    """
    
    def __init__(self, resource_limits, dir, waiter_port):
        super(Kernel, self).__init__()
        self.resource_limits = resource_limits
        self.dir = dir
        self.waiter_port = waiter_port

    def run(self):
        global logger
        logger = log.kernel_logger.getChild(str(os.getpid()))
        logger.debug("forked kernel is running")
        log.std_redirect(logger)
        # Close the global Context instance inherited from the parent process.
        zmq.Context.instance().term()
        # Become a group leader for cleaner exit.
        os.setpgrp()
        id = str(uuid.uuid4())
        dir = os.path.join(self.dir, id)
        try:
            os.mkdir(dir)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise
        os.chdir(dir)
        #config = traitlets.config.loader.Config({"ip": self.ip})
        #config.HistoryManager.enabled = False
        app = IPKernelApp.instance(log=logger)
        from namespace import InstrumentedNamespace
        app.user_ns = InstrumentedNamespace()
        app.initialize([])  # Redirects stdout/stderr
        #log.std_redirect(logger)   # Uncomment for debugging
        # This function should be called via atexit, but it isn't, perhaps due
        # to forking. Stale connection files do cause problems.
        app.cleanup_connection_file()
        kernel_init.initialize(app.kernel)
        for r, limit in self.resource_limits.iteritems():
            resource.setrlimit(getattr(resource, r), (limit, limit))
        logger.debug("kernel ready")
        context = zmq.Context.instance()
        socket = context.socket(zmq.PUSH)
        socket.setsockopt(zmq.IPV6, 1)
        socket.connect("tcp://localhost:{}".format(self.waiter_port))
        socket.send_json({
            "id": id,
            "connection": {
                "key": app.session.key,                
                "ip": app.ip,
                "hb": app.hb_port,
                "iopub": app.iopub_port,
                "shell": app.shell_port,
                },
            "limits": self.resource_limits,
            })
            
        def signal_handler(signum, frame):
            logger.info("received %s, shutting down", signum)
            app.kernel.do_shutdown(False)

        signal.signal(signal.SIGTERM, signal_handler)
        app.start()
        logger.debug("Kernel.run finished")

    def stop(self):
        pid = self.pid
        if self.is_alive():
            logger.debug("killing kernel process %d", pid)
            os.kill(pid, signal.SIGTERM)
            self.join(1)
            if self.is_alive():
                logger.warning(
                    "kernel process %d didn't stop after 1 second", pid)
        try:
            # Kernel PGID is the same as PID
            os.killpg(pid, signal.SIGKILL)
        except OSError as e:
            if e.errno !=  errno.ESRCH:
                raise
        logger.debug("killed kernel process group %d", pid)


class KernelProvider(object):
    r"""
    Kernel Provider handles compute kernels on the worker side.
    """
    
    def __init__(self, dealer_address, dir):
        self.is_active = False
        self.dir = dir
        context = zmq.Context.instance()
        self.dealer = context.socket(zmq.DEALER)
        self.dealer.setsockopt(zmq.IPV6, 1)
        logger.debug("connecting to %s", address)
        self.dealer.connect(address)
        self.dealer.send_json("get settings")
        if not self.dealer.poll(5000):
            logger.debug("dealer does not answer, terminating")
            exit(1)
        reply = self.dealer.recv_json()
        logger.debug("received %s", reply)
        assert reply[0] == "settings" and len(reply) == 2
        self.settings = reply[1]
        self.waiter = context.socket(zmq.PULL)
        self.waiter.setsockopt(zmq.IPV6, 1)
        self.waiter_port = self.waiter.bind_to_random_port("tcp://*")
        self.kernels = dict()
        setup_sage()

    def fork(self, resource_limits=None):
        r"""
        Start a new kernel by forking.
        
        INPUT:
        
        - ``resource_limits`` - dictionary with keys ``resource.RLIMIT_*``
        
        OUTPUT:
        
        - kernel ID and connection information (which includes the kernel IP,
          session key, and shell, heartbeat, stdin, and iopub port numbers)
        """
        logger.debug("fork with limits %s", resource_limits)
        resource_limits = resource_limits or {}
        kernel = Kernel(resource_limits, self.dir, self.waiter_port)
        kernel.start()
        logger.debug("after proc start in fork")
        reply = self.waiter.recv_json()
        self.kernels[reply["id"]] = kernel
        return reply

    def start(self):
        self.is_active = True
        self.dealer.send_json("ready")
        while self.is_active:
            msg = self.dealer.recv_json()
            logger.debug("received %s", msg)
            if msg == "disconnect":
                self.stop()
            if msg[0] == "get":
                # Should not wait here but instead poll both dealer and waiter and react
                kernel = self.fork(resource_limits=msg[1])
                self.dealer.send_json(["kernel", kernel])
                self.dealer.send_json("ready")
            if msg[0] == "stop":
                self.kernels.pop(msg[1]).stop()
        # When cleaning up, need to wait for all forked processes to finish
        # forking, or in fact there should be only one forking going on
        # at a time!
        while self.kernels:
            self.kernels.popitem()[1].stop()
            
    def stop(self):
        self.is_active = False


            
def setup_sage():
    import sage
    import sage.all
    # override matplotlib and pylab show functions
    # TODO: use something like IPython's inline backend
    
    def mp_show(savefig):
        filename = "%s.png" % uuid.uuid4()
        savefig(filename)
        msg = {"text/image-filename": filename}
        sys._sage_.sent_files[filename] = os.path.getmtime(filename)
        sys._sage_.display_message(msg)
        
    from functools import partial
    import pylab
    pylab.show = partial(mp_show, savefig=pylab.savefig)
    import matplotlib.pyplot
    matplotlib.pyplot.show = partial(mp_show, savefig=matplotlib.pyplot.savefig)

    # The first plot takes about 2 seconds to generate (presumably
    # because lots of things, like matplotlib, are imported).  We plot
    # something here so that worker processes don't have this overhead
    try:
        sage.all.plot(1, (0, 1))
    except Exception:
        logger.exception("plotting exception")


if __name__ == "__main__":        
    parser = argparse.ArgumentParser(
        description="Launch a kernel provider for SageMathCell")
    parser.add_argument("--address",
        help="address of the kernel dealer (defaults to $SSH_CLIENT)")
    parser.add_argument("port", type=int,
        help="port of the kernel dealer")
    parser.add_argument("dir",
        help="directory name for user files saved by kernels")
    args = parser.parse_args()

    log.std_redirect(logger)
    address = args.address or os.environ["SSH_CLIENT"].split()[0]
    if ":" in address:
        address = "[{}]".format(address)
    address = "tcp://{}:{}".format(address, args.port)
    provider = KernelProvider(address, args.dir)

    def signal_handler(signum, frame):
        logger.info("received %s, shutting down", signum)
        provider.stop()

    signal.signal(signal.SIGTERM, signal_handler)
    provider.start()
