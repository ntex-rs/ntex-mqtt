#![recursion_limit = "1024"]

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate error_chain;

extern crate mio;
extern crate slab;
extern crate bytes;
extern crate nom;
extern crate clap;

extern crate mqtt;

use std::mem;
use std::io::prelude::*;
use std::process::exit;
use std::net::ToSocketAddrs;

use nom::IError;
use mio::{Token, Poll, PollOpt, Ready, Events};
use mio::tcp::{TcpListener, TcpStream};
use slab::Slab;
use bytes::{Buf, BufMut, ByteBuf, SliceBuf};

use clap::{Arg, App};

use mqtt::{read_packet, WritePacketExt};

mod errors {
    error_chain!{
        types {
            Error, ErrorKind, ResultExt, Result;
        }

        foreign_links {
            Fmt(::std::fmt::Error);
            Io(::std::io::Error) #[cfg(unix)];
        }

        errors {
            InvalidAddress
            TooManyConnections
            InvalidState
            InvalidToken
            InvalidPacket(err: ::nom::IError)
        }
    }
}

use errors::*;

const SERVER: Token = Token(0);

const MIN_CONNECTIONS: usize = 1024;
const MAX_CONNECTIONS: usize = 1024 * 1024;

struct Server {
    sock: TcpListener,
    token: Token,
    conns: Slab<Connection>,
}

impl Server {
    fn new<T: ToSocketAddrs>(addr: T) -> Result<Server> {
        let listener = Self::bind(addr)?;

        info!("server listen on {}", listener.local_addr().unwrap());

        Ok(Server {
            sock: listener,
            token: SERVER,
            conns: Slab::with_capacity(MIN_CONNECTIONS),
        })
    }

    fn bind<T: ToSocketAddrs>(addr: T) -> Result<TcpListener> {
        let mut last_err = None;

        for addr in addr.to_socket_addrs()? {
            match TcpListener::bind(&addr) {
                Ok(l) => {
                    return Ok(l);
                }
                Err(err) => {
                    info!("fail to bind address {}, {}", addr, err);

                    last_err = Some(err)
                }
            }
        }

        if let Some(err) = last_err {
            bail!(err)
        } else {
            bail!(ErrorKind::InvalidAddress)
        }
    }

    fn serve(&mut self, poll: &Poll) -> Result<()> {
        // Start listening for incoming connections
        poll.register(&self.sock, self.token, Ready::readable(), PollOpt::edge())?;

        // Create storage for events
        let mut events = Events::with_capacity(1024);

        loop {
            poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                match event.token() {
                    SERVER => {
                        if let Err(err) = self.accept(&poll) {
                            warn!("fail to accept: {}", err);
                        }
                    }
                    token => {
                        match self.conns.entry(usize::from(token) - 1) {
                            Some(mut entry) => {
                                entry.get_mut().handle(event.kind(), poll)?;

                                if entry.get().is_closed() {
                                    entry.remove();
                                }
                            }
                            _ => bail!(ErrorKind::InvalidToken),
                        }
                    }
                }
            }
        }
    }

    fn accept(&mut self, poll: &Poll) -> Result<()> {
        let (conn, addr) = self.sock.accept()?;

        if !self.conns.has_available() {
            let additional = if self.conns.capacity() * 2 < MAX_CONNECTIONS {
                self.conns.capacity()
            } else {
                MAX_CONNECTIONS - self.conns.len()
            };

            info!("expand connection pool from {} to {}",
                  self.conns.len(),
                  self.conns.len() + additional);

            self.conns.reserve_exact(additional);
        }

        match self.conns.vacant_entry() {
            Some(entry) => {
                let token = Token(entry.index() + 1);

                entry.insert(Connection::new(conn, token)).get().register(poll)
            }
            None => {
                info!("drop connection from {}", addr);

                bail!(ErrorKind::TooManyConnections)
            }
        }
    }
}

enum State {
    Reading(ByteBuf),
    Writing(SliceBuf<Vec<u8>>, ByteBuf),
    Closed,
}

impl State {
    fn reading() -> State {
        State::Reading(ByteBuf::with_capacity(8 * 1024))
    }

    fn read_buf(&self) -> &[u8] {
        match *self {
            State::Reading(ref buf) => buf.bytes(),
            _ => panic!("connection not in reading state"),
        }
    }

    fn mut_read_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Reading(ref mut buf) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in reading state"),
        }
    }

    fn write_buf(&self) -> &[u8] {
        match *self {
            State::Writing(ref buf, _) => buf.bytes(),
            _ => panic!("connection not in writing state"),
        }
    }

    fn mut_write_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Writing(ref mut buf, _) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in writing state"),
        }
    }

    fn unwrap_read_buf(self) -> ByteBuf {
        match self {
            State::Reading(buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    fn unwrap_write_buf(self) -> (SliceBuf<Vec<u8>>, ByteBuf) {
        match self {
            State::Writing(buf, remaining) => (buf, remaining),
            _ => panic!("connection not in writing state"),
        }
    }

    fn try_transition_to_writing(&mut self, n: usize) -> Result<()> {
        let mut buf = mem::replace(self, State::Closed).unwrap_read_buf();

        unsafe {
            buf.advance_mut(n);
        }

        match read_packet(buf.bytes()) {
            Ok((remaining, packet)) => {
                debug!("decoded request packet {:?}", packet);

                let mut data = Vec::with_capacity(1024);

                data.write_packet(&packet)?;

                debug!("encoded response packet {:?} in {} bytes",
                       packet,
                       data.len());

                *self = State::Writing(SliceBuf::new(data), ByteBuf::from_slice(remaining));
            }
            Err(IError::Incomplete(_)) => {
                debug!("packet incomplete, read again");
            }
            Err(err) => {
                warn!("fail to parse packet, {:?}", err);

                bail!(ErrorKind::InvalidPacket(err));
            }
        }

        Ok(())
    }

    fn try_transition_to_reading(&mut self, n: usize) {
        let (mut buf, remaining) = mem::replace(self, State::Closed).unwrap_write_buf();

        if buf.remaining() > n {
            buf.advance(n);

            *self = State::Writing(buf, remaining);
        } else {
            let mut buf = ByteBuf::with_capacity(8 * 1024);

            buf.copy_from_slice(remaining.bytes());

            *self = State::Reading(buf);
        }
    }
}

struct Connection {
    sock: TcpStream,
    token: Token,
    state: State,
}

impl Connection {
    fn new(conn: TcpStream, token: Token) -> Connection {
        debug!("connection {:?} created ({} -> {})",
               token,
               conn.peer_addr().unwrap(),
               conn.local_addr().unwrap());

        Connection {
            sock: conn,
            token: token,
            state: State::reading(), // TODO allocate from pool
        }
    }

    fn close(&mut self) {
        debug!("connection {:?} closed", self.token);

        self.state = State::Closed;
    }

    fn is_closed(&self) -> bool {
        match self.state {
            State::Closed => true,
            _ => false,
        }
    }

    fn handle(&mut self, ready: Ready, poll: &Poll) -> Result<()> {
        match self.state {
            _ if ready.is_hup() => {
                self.close();

                Ok(())
            }

            State::Reading(..) if ready.is_readable() => {
                self.handle_read().and(self.register(poll))
            }

            State::Writing(..) if ready.is_writable() => {
                self.handle_write().and(self.register(poll))
            }

            _ => bail!(ErrorKind::InvalidState),
        }
    }

    fn handle_read(&mut self) -> Result<()> {
        match self.sock.read(self.state.mut_read_buf()) {
            Ok(0) => {
                debug!("read 0 bytes from client, buffered {} bytes",
                       self.state.read_buf().len());

                match self.state.read_buf().len() {
                    n if n > 0 => {
                        self.state.try_transition_to_writing(0)?;
                    }
                    _ => {
                        self.close();
                    }
                }
            }
            Ok(n) => {
                debug!("read {} bytes from client", n);

                self.state.try_transition_to_writing(n)?;
            }
            Err(err) => {
                warn!("read failed, {}", err);
            }
        }

        Ok(())
    }

    fn handle_write(&mut self) -> Result<()> {
        match self.sock.write(self.state.mut_write_buf()) {
            Ok(0) => debug!("wrote 0 bytes to client, try again later"),
            Ok(n) => {
                debug!("wrote {} of {} bytes to client",
                       n,
                       self.state.write_buf().len());

                self.state.try_transition_to_reading(n);
            }
            Err(err) => {
                warn!("write failed, {}", err);
            }
        }

        Ok(())
    }

    fn register(&self, poll: &Poll) -> Result<()> {
        poll.register(&self.sock,
                      self.token,
                      match self.state {
                          State::Reading(..) => Ready::readable(),
                          State::Writing(..) => Ready::writable(),
                          _ => Ready::none(),
                      },
                      PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }
}

const DEFAULT_HOST: &'static str = "localhost";
const DEFAULT_PORT: &'static str = "1883";

fn main() {
    let _ = env_logger::init().unwrap();

    let matches = App::new("Echo Server")
        .version("1.0")
        .author("Flier Lu <flier.lu@gmail.com>")
        .arg(Arg::with_name("listen")
            .short("l")
            .value_name("HOST")
            .default_value(DEFAULT_HOST)
            .help("listen on the host"))
        .arg(Arg::with_name("port")
            .short("p")
            .value_name("PORT")
            .default_value(DEFAULT_PORT)
            .help("listen on the port"))
        .get_matches();

    let addr = (matches.value_of("listen").unwrap(),
                matches.value_of("port").unwrap().parse().unwrap());

    let mut server = Server::new(addr).unwrap();
    let poll = Poll::new().unwrap();

    if let Err(ref err) = server.serve(&poll) {
        error!("error: {}", err);

        for err in err.iter().skip(1) {
            error!("caused by: {}", err);
        }

        if let Some(backtrace) = err.backtrace() {
            warn!("backtrace: {:?}", backtrace);
        }

        exit(-1);
    }
}
