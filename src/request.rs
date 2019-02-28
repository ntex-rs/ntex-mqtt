use actix_codec::{AsyncRead, AsyncWrite, Framed};
use mqtt_codec as mqtt;

pub struct Request<Io, U, T> {
    framed: Framed<Io, U>,
    param: T,
}

impl<Io> Request<Io, mqtt::Codec, ()>
where
    Io: AsyncRead + AsyncWrite,
{
    pub fn new(io: Io) -> Self {
        Request {
            framed: Framed::new(io, mqtt::Codec::new()),
            param: (),
        }
    }
}

impl<Io, U, T> Request<Io, U, T> {
    pub fn framed(framed: Framed<Io, U>, param: T) -> Self {
        Request { framed, param }
    }

    pub fn param<T1>(self, param: T1) -> Request<Io, U, T1> {
        Request {
            param,
            framed: self.framed,
        }
    }

    pub(crate) fn into_parts(self) -> (Framed<Io, U>, T) {
        (self.framed, self.param)
    }
}

impl<Io, U, T> IntoRequest<Io, U, T> for Request<Io, U, T> {
    fn into_request(self) -> Self {
        self
    }
}

pub trait IntoRequest<Io, U, T> {
    fn into_request(self) -> Request<Io, U, T>;
}

impl<Io, U> IntoRequest<Io, U, ()> for Framed<Io, U> {
    fn into_request(self) -> Request<Io, U, ()> {
        Request {
            framed: self,
            param: (),
        }
    }
}

impl<Io> IntoRequest<Io, mqtt::Codec, ()> for Io
where
    Io: AsyncRead + AsyncWrite,
{
    fn into_request(self) -> Request<Io, mqtt::Codec, ()> {
        Request {
            framed: Framed::new(self, mqtt::Codec::new()),
            param: (),
        }
    }
}
