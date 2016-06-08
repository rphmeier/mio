use std::fmt;
use std::io::{self, Read, Write, Cursor};
use std::mem;
use std::net::{self, SocketAddr};
use std::os::windows::prelude::*;
use std::sync::{Mutex, MutexGuard};

use net::tcp::Shutdown;
use miow::iocp::CompletionStatus;
use miow::net::*;
use miow::pipe as miowpipe;
use winapi::*;

use {Evented, EventSet, PollOpt, Selector, Token};
use event::IoEvent;
use sys::windows::selector::{Overlapped, Registration};
use sys::windows::{wouldblock, Family};
use sys::windows::from_raw_arc::FromRawArc;

pub struct NamedPipe {
    /// Separately stored implementation to ensure that the `Drop`
    /// implementation on this type is only executed when it's actually dropped
    /// (many clones of this `imp` are made).
    imp: PipeImp,
}

pub struct PipeListener {
    imp: ListenerImp,
}

#[derive(Clone)]
struct PipeImp {
    /// A stable address and synchronized access for all internals. This serves
    /// to ensure that all `Overlapped` pointers are valid for a long period of
    /// time as well as allowing completion callbacks to have access to the
    /// internals without having ownership.
    ///
    /// Note that the reference count also allows us "loan out" copies to
    /// completion ports while I/O is running to guarantee that this stays alive
    /// until the I/O completes. You'll notice a number of calls to
    /// `mem::forget` below, and these only happen on successful scheduling of
    /// I/O and are paired with `overlapped2arc!` macro invocations in the
    /// completion callbacks (to have a decrement match the increment).
    inner: FromRawArc<StreamIo>,
}

#[derive(Clone)]
struct ListenerImp {
    inner: FromRawArc<ListenerIo>,
}

struct StreamIo {
    inner: Mutex<StreamInner>,
    read: Overlapped, // also used for connect
    write: Overlapped,
}

struct ListenerIo {
    inner: Mutex<ListenerInner>,
    accept: Overlapped,
}

struct StreamInner {
	endpoint: String,
    pipe: miowpipe::NamedPipe,
    iocp: Registration,
    read: State<Vec<u8>, Cursor<Vec<u8>>>,
    write: State<(Vec<u8>, usize), (Vec<u8>, usize)>,
}

struct ListenerInner {
	endpoint: String,
	accept: State<miowpipe::NamedPipe, miowpipe::NamedPipe>,
    iocp: Registration,
}

enum State<T, U> {
    Empty,              // no I/O operation in progress
    Pending(T),         // an I/O operation is in progress
    Ready(U),           // I/O has finished with this value
    Error(io::Error),   // there was an I/O error
}

impl NamedPipe {
    fn new(endpoint: &str) -> Result<NamedPipe> {
		let pipe = try!(miowpipe::NamedPipeBuilder::new(endpoint).create());
        NamedPipe {
            imp: PipeImp {
                inner: FromRawArc::new(StreamIo {
                    read: Overlapped::new(read_done),
                    write: Overlapped::new(write_done),
                    inner: Mutex::new(StreamInner {
						endpoint: endpoint,
                        pipe: pipe,
                        iocp: Registration::new(),
                        deferred_connect: deferred_connect,
                        read: State::Empty,
                        write: State::Empty,
                    }),
                }),
            },
        }
    }

    pub fn connect(endpoint: &str) -> io::Result<NamedPipe> {
        NamedPipe::new(endpoint)
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.inner().pipe.disconnect()
    }

    fn inner(&self) -> MutexGuard<StreamInner> {
        self.imp.inner()
    }

    fn post_register(&self, interest: EventSet, me: &mut StreamInner) {
        if interest.is_readable() {
            self.imp.schedule_read(me);
        }

        // At least with epoll, if a pipe is registered with an interest in
        // writing and it's immediately writable then a writable event is
        // generated immediately, so do so here.
        if interest.is_writable() {
            if let State::Empty = me.write {
                me.iocp.defer(EventSet::writable());
            }
        }
    }
}

impl PipeImp {
    fn inner(&self) -> MutexGuard<StreamInner> {
        self.inner.inner.lock().unwrap()
    }

    fn schedule_connect(&self, me: &mut StreamInner)
                        -> io::Result<()> {
        unsafe {
            trace!("scheduling a connect");
            try!(me.pipe.connect_overlapped(self.inner.read.get_mut()));
        }
        // see docs above on PipeImp.inner for rationale on forget
        mem::forget(self.clone());
        Ok(())
    }

    /// Issues a "read" operation for this pipe, if applicable.
    ///
    /// This is intended to be invoked from either a completion callback or a
    /// normal context. The function is infallible because errors are stored
    /// internally to be returned later.
    ///
    /// It is required that this function is only called after the handle has
    /// been registered with an event loop.
    fn schedule_read(&self, me: &mut StreamInner) {
        match me.read {
            State::Empty => {}
            _ => return,
        }

        me.iocp.unset_readiness(EventSet::readable());

        let mut buf = me.iocp.get_buffer(64 * 1024);
        let res = unsafe {
            trace!("scheduling a read");
            let cap = buf.capacity();
            buf.set_len(cap);
            me.pipe.read_overlapped(&mut buf, self.inner.read.get_mut())
        };
        match res {
            Ok(_) => {
                // see docs above on PipeImp.inner for rationale on forget
                me.read = State::Pending(buf);
                mem::forget(self.clone());
            }
            Err(e) => {
                let mut set = EventSet::readable();
				//TODO:
                if e.raw_os_error() == Some(WSAECONNRESET as i32) {
                    set = set | EventSet::hup();
                }
                me.read = State::Error(e);
                me.iocp.defer(set);
                me.iocp.put_buffer(buf);
            }
        }
    }

    /// Similar to `schedule_read`, except that this issues, well, writes.
    ///
    /// This function will continually attempt to write the entire contents of
    /// the buffer `buf` until they have all been written. The `pos` argument is
    /// the current offset within the buffer up to which the contents have
    /// already been written.
    ///
    /// A new writable event (e.g. allowing another write) will only happen once
    /// the buffer has been written completely (or hit an error).
    fn schedule_write(&self, buf: Vec<u8>, pos: usize,
                      me: &mut StreamInner) {

        // About to write, clear any pending level triggered events
        me.iocp.unset_readiness(EventSet::writable());

        trace!("scheduling a write");
        let err = unsafe {
            me.pipe.write_overlapped(&buf[pos..], self.inner.write.get_mut())
        };
        match err {
            Ok(_) => {
                // see docs above on PipeImp.inner for rationale on forget
                me.write = State::Pending((buf, pos));
                mem::forget(self.clone());
            }
            Err(e) => {
                me.write = State::Error(e);
                me.iocp.defer(EventSet::writable());
                me.iocp.put_buffer(buf);
            }
        }
    }

    /// Pushes an event for this pipe onto the selector its registered for.
    ///
    /// When an event is generated on this pipe, if it happened after the
    /// pipe was closed then we don't want to actually push the event onto our
    /// selector as otherwise it's just a spurious notification.
    fn push(&self, me: &mut StreamInner, set: EventSet,
            into: &mut Vec<IoEvent>) {
        if me.pipe.as_raw_handle() != INVALID_PIPE {
            me.iocp.push_event(set, into);
        }
    }
}

fn read_done(status: &CompletionStatus, dst: &mut Vec<IoEvent>) {
    let me2 = PipeImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), StreamIo, read) },
    };

    let mut me = me2.inner();
    match mem::replace(&mut me.read, State::Empty) {
        State::Pending(mut buf) => {
            trace!("finished a read: {}", status.bytes_transferred());

            unsafe {
                buf.set_len(status.bytes_transferred() as usize);
            }

            me.read = State::Ready(Cursor::new(buf));

            // If we transferred 0 bytes then be sure to indicate that hup
            // happened.
            let mut e = EventSet::readable();

            return me2.push(&mut me, e, dst)
        }
        s => me.read = s,
    }

    // If a read didn't complete, then the connect must have just finished.
    trace!("finished a connect");
    me2.push(&mut me, EventSet::writable(), dst);
    me2.schedule_read(&mut me);
}

fn write_done(status: &CompletionStatus, dst: &mut Vec<IoEvent>) {
    trace!("finished a write {}", status.bytes_transferred());
    let me2 = PipeImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), StreamIo, write) },
    };
    let mut me = me2.inner();
    let (buf, pos) = match mem::replace(&mut me.write, State::Empty) {
        State::Pending(pair) => pair,
        _ => unreachable!(),
    };
    let new_pos = pos + (status.bytes_transferred() as usize);
    if new_pos == buf.len() {
        me2.push(&mut me, EventSet::writable(), dst);
    } else {
        me2.schedule_write(buf, new_pos, &mut me);
    }
}

impl Read for NamedPipe {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut me = self.inner();

        match mem::replace(&mut me.read, State::Empty) {
            State::Empty => Err(wouldblock()),
            State::Pending(buf) => {
                me.read = State::Pending(buf);
                Err(wouldblock())
            }
            State::Ready(mut cursor) => {
                let amt = try!(cursor.read(buf));
                // Once the entire buffer is written we need to schedule the
                // next read operation.
                if cursor.position() as usize == cursor.get_ref().len() {
                    me.iocp.put_buffer(cursor.into_inner());
                    self.imp.schedule_read(&mut me);
                } else {
                    me.read = State::Ready(cursor);
                }
                Ok(amt)
            }
            State::Error(e) => {
                self.imp.schedule_read(&mut me);
                Err(e)
            }
        }
    }
}

impl Write for NamedPipe {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut me = self.inner();
        let me = &mut *me;

        match me.write {
            State::Empty => {}
            _ => return Err(wouldblock())
        }

        if me.iocp.port().is_none() {
            return Err(wouldblock())
        }

        let mut intermediate = me.iocp.get_buffer(64 * 1024);
        let amt = try!(intermediate.write(buf));
        self.imp.schedule_write(intermediate, 0, me);
        Ok(amt)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Evented for NamedPipe {
    fn register(&self, selector: &mut Selector, token: Token,
                interest: EventSet, opts: PollOpt) -> io::Result<()> {
        let mut me = self.inner();
        let me = &mut *me;
        try!(me.iocp.register_handle(&me.pipe, selector, token, interest,
                                     opts));

        // If we were connected before being registered process that request
        // here and go along our merry ways. Note that the callback for a
        // successful connect will worry about generating writable/readable
        // events and scheduling a new read.
        if let Some(addr) = me.deferred_connect.take() {
            return self.imp.schedule_connect(&addr, me).map(|_| ())
        }
        self.post_register(interest, me);
        Ok(())
    }

    fn reregister(&self, selector: &mut Selector, token: Token,
                  interest: EventSet, opts: PollOpt) -> io::Result<()> {
        let mut me = self.inner();
        {
            let me = &mut *me;
            try!(me.iocp.reregister(token, interest, opts));
        }
        self.post_register(interest, &mut me);
        Ok(())
    }

    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.inner().iocp.checked_deregister(selector)
    }
}

impl fmt::Debug for NamedPipe {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "NamedPipe { ... }".fmt(f)
    }
}

impl Drop for NamedPipe {
    fn drop(&mut self) {
        let mut inner = self.inner();
        // When the `NamedPipe` itself is dropped then we close the internal
        // handle (e.g. call `closepipe`). This will cause all pending I/O
        // operations to forcibly finish and we'll get notifications for all of
        // them and clean up the rest of our internal state (yay!).
        //
        // This is achieved by replacing our pipe with an invalid one, so all
        // further operations will return an error (but no further operations
        // should be done anyway).
        inner.pipe = unsafe {
            miowpipe::NamedPipe::from_raw_handle(INVALID_PIPE)
        };

        // Then run any finalization code including level notifications
        inner.iocp.deregister();
    }
}

impl PipeListener {
    pub fn new(endpoint: &str) -> io::Result<PipeListener> {
        PipeListener {
            imp: ListenerImp {
                inner: FromRawArc::new(ListenerIo {
                    accept: Overlapped::new(accept_done),
                    inner: Mutex::new(ListenerInner {
                        endpoint: endpoint.to_owned(),
                        iocp: Registration::new(),
                        accept: State::Empty,
                    }),
                }),
            },
        }
    }

    pub fn accept(&self) -> io::Result<Option<NamedPipe>> {
        let mut me = self.inner();

        let ret = match mem::replace(&mut me.accept, State::Empty) {
            State::Empty => return Ok(None),
            State::Pending(t) => {
                me.accept = State::Pending(t);
                return Ok(None)
            }
            State::Ready(p) => {
                Ok(Some(NamedPipe::new(p, me.endpoint)))
            }
            State::Error(e) => Err(e),
        };

        self.imp.schedule_accept(&mut me);

        return ret
    }

    fn inner(&self) -> MutexGuard<ListenerInner> {
        self.imp.inner()
    }
}

impl ListenerImp {
    fn inner(&self) -> MutexGuard<ListenerInner> {
        self.inner.inner.lock().unwrap()
    }

    fn schedule_accept(&self, me: &mut ListenerInner) {
        match me.accept {
            State::Empty => {}
            _ => return
        }

        me.iocp.unset_readiness(EventSet::readable());

		let pipe = try!(miowpipe::NamedPipeBuilder::new(endpoint).create());
        match pipe.connect_overlapped(self.inner.accept.get_mut()) {
            Ok(_) => {
                // see docs above on StreamImp.inner for rationale on forget
                me.accept = State::Pending(pipe);
                mem::forget(self.clone());
            }
            Err(e) => {
                me.accept = State::Error(e);
                me.iocp.defer(EventSet::readable());
            }
        }
    }

    // See comments in StreamImp::push
    fn push(&self, me: &mut ListenerInner, set: EventSet,
            into: &mut Vec<IoEvent>) {
        if me.pipe.as_raw_handle() != INVALID_PIPE {
            me.iocp.push_event(set, into);
        }
    }
}

fn accept_done(status: &CompletionStatus, dst: &mut Vec<IoEvent>) {
    let me2 = ListenerImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), ListenerIo, accept) },
    };

    let mut me = me2.inner();
    let pipe = match mem::replace(&mut me.accept, State::Empty) {
        State::Pending(s) => s,
        _ => unreachable!(),
    };
    trace!("finished an accept");
    me.accept = State::Ready(pipe);
    me2.push(&mut me, EventSet::readable(), dst);
}

impl Evented for PipeListener {
    fn register(&self, selector: &mut Selector, token: Token,
                interest: EventSet, opts: PollOpt) -> io::Result<()> {
        let mut me = self.inner();
        let me = &mut *me;
        try!(me.iocp.register_socket(&me.socket, selector, token, interest,
                                     opts));
        self.imp.schedule_accept(me);
        Ok(())
    }

    fn reregister(&self, selector: &mut Selector, token: Token,
                  interest: EventSet, opts: PollOpt) -> io::Result<()> {
        let mut me = self.inner();
        let me = &mut *me;
        try!(me.iocp.reregister_socket(&me.socket, selector, token,
                                       interest, opts));
        self.imp.schedule_accept(me);
        Ok(())
    }

    fn deregister(&self, selector: &mut Selector) -> io::Result<()> {
        self.inner().iocp.checked_deregister(selector)
    }
}

impl fmt::Debug for PipeListener {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "PipeListener { ... }".fmt(f)
    }
}

impl Drop for PipeListener {
    fn drop(&mut self) {
        // Run any finalization code including level notifications
        inner.iocp.deregister();
    }
}

