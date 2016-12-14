use std::mem;
use std::io;
use std::io::prelude::*;
use std::net::{self, SocketAddr};
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, ByteBuf, SliceBuf};

use nom::IError;

use rotor::{EventSet, PollOpt, Void};
use rotor::mio::tcp::{TcpListener, TcpStream};
use rotor::{Machine, Response, EarlyScope, Scope, GenericScope};

use error::*;
use packet::Packet;
use decode::read_packet;
use encode::WritePacketExt;

pub trait Handler<'a> {
    fn on_received_packet(&mut self, packet: &Packet<'a>);
}

pub trait Transport {
    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        Ok(())
    }
}

impl Transport for Tcp {}
impl Transport for Udp {}
impl Transport for Tls {}
impl Transport for WebSocket {}

pub enum State {
    Receiving(ByteBuf),
    Sending(SliceBuf<Vec<u8>>, ByteBuf),
    Closed,
}

impl State {
    pub fn receiving() -> State {
        State::Receiving(ByteBuf::with_capacity(8 * 1024))
    }

    #[inline]
    fn read_buf(&self) -> &[u8] {
        match *self {
            State::Receiving(ref buf) => buf.bytes(),
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn mut_read_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Receiving(ref mut buf) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn write_buf(&self) -> &[u8] {
        match *self {
            State::Sending(ref buf, _) => buf.bytes(),
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    fn mut_write_buf(&mut self) -> &mut [u8] {
        match *self {
            State::Sending(ref mut buf, _) => unsafe { buf.bytes_mut() },
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    fn unwrap_read_buf(self) -> ByteBuf {
        match self {
            State::Receiving(buf) => buf,
            _ => panic!("connection not in reading state"),
        }
    }

    #[inline]
    fn unwrap_write_buf(self) -> (SliceBuf<Vec<u8>>, ByteBuf) {
        match self {
            State::Sending(buf, remaining) => (buf, remaining),
            _ => panic!("connection not in writing state"),
        }
    }

    #[inline]
    pub fn async_read<R: Read>(&mut self, r: &mut R) -> Result<()> {
        match r.read(self.mut_read_buf()) {
            Ok(0) => {
                debug!("read 0 bytes from client, buffered {} bytes",
                       self.read_buf().len());

                match self.read_buf().len() {
                    n if n > 0 => self.try_transition_to_writing(0),
                    _ => {
                        *self = State::Closed;
                        Ok(())
                    }
                }
            }
            Ok(n) => {
                debug!("read {} bytes from client", n);

                self.try_transition_to_writing(n)
            }
            Err(err) => {
                warn!("read failed, {}", err);

                bail!(err)
            }
        }
    }

    #[inline]
    pub fn async_write<W: Write>(&mut self, w: &mut W) -> Result<()> {
        match w.write(self.mut_write_buf()) {
            Ok(0) => {
                debug!("wrote 0 bytes to client, try again later");

                Ok(())
            }
            Ok(n) => {
                debug!("wrote {} of {} bytes to client", n, self.write_buf().len());

                self.try_transition_to_reading(n);

                Ok(())
            }
            Err(err) => {
                warn!("write failed, {}", err);

                bail!(err)
            }
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

                *self = State::Sending(SliceBuf::new(data), ByteBuf::from_slice(remaining));

                Ok(())
            }
            Err(IError::Incomplete(_)) => {
                debug!("packet incomplete, read again");

                Ok(())
            }
            Err(err) => {
                warn!("fail to parse packet, {:?}", err);

                bail!(ErrorKind::InvalidPacket)
            }
        }
    }

    fn try_transition_to_reading(&mut self, n: usize) {
        let (mut buf, remaining) = mem::replace(self, State::Closed).unwrap_write_buf();

        if buf.remaining() > n {
            buf.advance(n);

            *self = State::Sending(buf, remaining);
        } else {
            let mut buf = ByteBuf::with_capacity(8 * 1024);

            buf.copy_from_slice(remaining.bytes());

            *self = State::Receiving(buf);
        }
    }
}

pub struct TcpContext;

pub enum Tcp {
    Server(TcpListener),
    Client(TcpStream),
    Connection(TcpStream, State),
}

pub struct Fsm(Arc<Mutex<Tcp>>);
pub struct TcpClient(Arc<Mutex<Tcp>>);

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

    fn wrap_listener(res: io::Result<TcpListener>, scope: &mut EarlyScope) -> Response<Self, Void> {
        match res.and_then(|sock| {
                info!("tcp server listen on {}", sock.local_addr()?);

                Ok(Tcp::Server(sock))
            })
            .and_then(|m| m.register(scope).map(|_| m)) {
            Ok(m) => Response::ok(m),
            Err(err) => Response::error(Box::new(err)),
        }
    }

    pub fn client(addr: &SocketAddr, scope: &mut EarlyScope) -> Response<(Fsm, TcpClient), Void> {
        Self::wrap_stream(TcpStream::connect(addr), scope)
    }

    pub fn from_stream(stream: net::TcpStream,
                       addr: &SocketAddr,
                       scope: &mut EarlyScope)
                       -> Response<(Fsm, TcpClient), Void> {
        Self::wrap_stream(TcpStream::connect_stream(stream, addr), scope)
    }

    fn wrap_stream(res: io::Result<TcpStream>,
                   scope: &mut EarlyScope)
                   -> Response<(Fsm, TcpClient), Void> {
        match res.and_then(|sock| {
                info!("tcp stream {} -> {}", sock.local_addr()?, sock.peer_addr()?);

                Ok(Tcp::Client(sock))
            })
            .and_then(|m| m.register(scope).map(|_| m)) {
            Ok(m) => {
                let arc = Arc::new(Mutex::new(m));

                Response::ok((Fsm(arc.clone()), TcpClient(arc.clone())))
            }
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

    fn register<S: GenericScope>(&self, scope: &mut S) -> io::Result<()> {
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

pub struct UdpContext;
pub enum Udp {}

pub struct TlsContext;
pub enum Tls {}

pub struct WebSocketContext;
pub enum WebSocket {}
