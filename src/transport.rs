use std::io::Result;
use std::net::{self, SocketAddr};

use rotor::{EventSet, PollOpt, Void};
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor::{Machine, Response, EarlyScope, Scope, GenericScope};

use proto::*;

pub struct TcpContext;

pub enum Tcp {
    Server(TcpListener),
    Client(TcpStream),
    Connection(TcpStream, State),
}

impl Tcp {
    pub fn server(addr: &SocketAddr, scope: &mut EarlyScope) -> Response<Self, Void> {
        Self::wrap_listener(TcpListener::bind(addr), scope)
    }

    pub fn from_listener(listener: net::TcpListener,
                         addr: &SocketAddr,
                         scope: &mut EarlyScope)
                         -> Response<Self, Void> {
        Self::wrap_listener(TcpListener::from_listener(listener, addr), scope)
    }

    fn wrap_listener(res: Result<TcpListener>, scope: &mut EarlyScope) -> Response<Self, Void> {
        match res.and_then(|sock| {
                info!("tcp server listen on {}", sock.local_addr()?);

                Ok(Tcp::Server(sock))
            })
            .and_then(|m| m.register(scope).map(|_| m)) {
            Ok(m) => Response::ok(m),
            Err(err) => Response::error(Box::new(err)),
        }
    }

    pub fn client(addr: &SocketAddr, scope: &mut EarlyScope) -> Response<Self, Void> {
        Self::wrap_stream(TcpStream::connect(addr), scope)
    }

    pub fn from_stream(stream: net::TcpStream,
                       addr: &SocketAddr,
                       scope: &mut EarlyScope)
                       -> Response<Self, Void> {
        Self::wrap_stream(TcpStream::connect_stream(stream, addr), scope)
    }

    fn wrap_stream(res: Result<TcpStream>, scope: &mut EarlyScope) -> Response<Self, Void> {
        match res.and_then(|sock| {
                info!("tcp stream {} -> {}", sock.local_addr()?, sock.peer_addr()?);

                Ok(Tcp::Client(sock))
            })
            .and_then(|m| m.register(scope).map(|_| m)) {
            Ok(m) => Response::ok(m),
            Err(err) => Response::error(Box::new(err)),
        }
    }

    fn accept(self) -> Response<Self, TcpStream> {
        match self {
            Tcp::Server(sock) => {
                match sock.accept() {
                    Ok(None) => {
                        debug!("accept none, try again");

                        Response::ok(Tcp::Server(sock))
                    }
                    Ok(Some((conn, addr))) => {
                        debug!("accept connection from {}", addr);

                        Response::spawn(Tcp::Server(sock), conn)
                    }
                    Err(err) => {
                        warn!("fail to accept connection, {}", err);

                        Response::ok(Tcp::Server(sock))
                    }
                }
            }
            _ => {
                error!("invalid state when accept");

                Response::done()
            }
        }
    }

    fn register<S: GenericScope>(&self, scope: &mut S) -> Result<()> {
        match *self {
            Tcp::Server(ref sock) => {
                debug!("server register for readable");

                scope.register(sock, EventSet::readable(), PollOpt::edge())
            }
            Tcp::Client(ref sock) => {
                debug!("client register for writable");

                scope.register(sock,
                               EventSet::writable(),
                               PollOpt::edge() | PollOpt::oneshot())
            }
            Tcp::Connection(ref sock, ref state) => {
                match *state {
                    State::Receiving(..) => {
                        debug!("connection register for readable");

                        scope.register(sock,
                                       EventSet::readable(),
                                       PollOpt::edge() | PollOpt::oneshot())
                    }
                    State::Sending(..) => {
                        debug!("connection register for writable");

                        scope.register(sock,
                                       EventSet::writable(),
                                       PollOpt::edge() | PollOpt::oneshot())
                    }
                    _ => Ok(()),
                }
            }
        }
    }
}

impl Machine for Tcp {
    type Seed = TcpStream;
    type Context = TcpContext;

    fn create(conn: TcpStream, scope: &mut Scope<TcpContext>) -> Response<Self, Void> {
        match Ok(Tcp::Connection(conn, State::receiving()))
            .and_then(|m| m.register(scope).map(|_| m)) {
            Ok(m) => Response::ok(m),
            Err(err) => Response::error(Box::new(err)),
        }
    }

    fn ready(self, events: EventSet, scope: &mut Scope<TcpContext>) -> Response<Self, Self::Seed> {
        match self {
            Tcp::Server(..) => self.accept(),
            Tcp::Client(sock) => Response::ok(Tcp::Connection(sock, State::receiving())),
            Tcp::Connection(mut sock, mut state) => {
                match state {
                    _ if events.is_hup() => Response::done(),

                    State::Receiving(..) if events.is_readable() => {
                        state.async_read(&mut sock)
                            .map(|_| Tcp::Connection(sock, state))
                            .and_then(|m| {
                                m.register(scope)?;
                                Ok(m)
                            })
                            .map(|m| Response::ok(m))
                            .unwrap_or(Response::done())
                    }

                    State::Sending(..) if events.is_writable() => {
                        state.async_write(&mut sock)
                            .map(|_| Tcp::Connection(sock, state))
                            .and_then(|m| {
                                m.register(scope)?;
                                Ok(m)
                            })
                            .map(|m| Response::ok(m))
                            .unwrap_or(Response::done())
                    }

                    _ => {
                        error!("invalid state when ready");

                        Response::done()
                    }
                }
            }
        }
    }

    fn spawned(self, _: &mut Scope<TcpContext>) -> Response<Self, Self::Seed> {
        match self {
            Tcp::Server(..) => self.accept(),
            _ => {
                error!("invalid state when spawned");

                Response::done()
            }
        }
    }

    fn timeout(self, _: &mut Scope<TcpContext>) -> Response<Self, Self::Seed> {
        info!("operation timeout");

        Response::done()
    }

    fn wakeup(self, _: &mut Scope<TcpContext>) -> Response<Self, Self::Seed> {
        info!("connection wakeup");

        Response::done()
    }
}
