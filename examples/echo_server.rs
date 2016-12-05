#![recursion_limit = "1024"]

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate error_chain;

extern crate mio;
extern crate slab;
extern crate bytes;

extern crate clap;

use std::process::exit;
use std::net::ToSocketAddrs;

use mio::{Token, Poll, PollOpt, Ready, Events};
use mio::tcp::{TcpListener, TcpStream};
use slab::Slab;
use bytes::BytesMut;

use clap::{Arg, App};

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
        }
    }
}

use errors::*;

const SERVER: Token = Token(0);

const MIN_CONNECTIONS: usize = 1024;
const MAX_CONNECTIONS: usize = 1024 * 1024;
const CONNECTION_BUFSIZE: usize = 1024 * 16;

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
                        let idx = usize::from(token) - 1;

                        if let Some(entry) = self.conns.entry(idx) {
                            if event.kind().is_hup() {
                                entry.remove().close()?;
                            } else if event.kind().is_readable() {

                            } else if event.kind().is_writable() {

                            }
                        } else {
                            error!("invalid connection token: {:?}", token);
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

                entry.insert(Connection::new(conn, token)).get().serve(poll)?;

                Ok(())
            }
            None => {
                info!("drop connection from {}", addr);

                bail!(ErrorKind::TooManyConnections)
            }
        }
    }
}

struct Connection {
    sock: TcpStream,
    token: Token,
    buf: BytesMut,
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
            buf: BytesMut::with_capacity(CONNECTION_BUFSIZE),
        }
    }

    fn close(&self) -> Result<()> {
        debug!("connection {:?} closed", self.token);

        Ok(())
    }

    fn serve(&self, poll: &Poll) -> Result<()> {
        debug!("connection {:?} opened", self.token);

        poll.register(&self.sock,
                      self.token,
                      Ready::readable(),
                      PollOpt::edge() | PollOpt::oneshot())?;

        Ok(())
    }
}

fn main() {
    let _ = env_logger::init().unwrap();

    let matches = App::new("Echo Server")
        .version("1.0")
        .author("Flier Lu <flier.lu@gmail.com>")
        .arg(Arg::with_name("listen")
            .short("l")
            .value_name("HOST")
            .default_value("localhost")
            .help("listen on the host"))
        .arg(Arg::with_name("port")
            .short("p")
            .value_name("PORT")
            .default_value("6567")
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
