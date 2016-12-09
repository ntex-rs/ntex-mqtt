use std::convert::From;

error_chain!{
    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error);
    }

    errors {
        OutOfMemory
        InvalidState
        InvalidPacket
        InvalidTopic
        SpawnError
    }
}

impl<T> From<::rotor::SpawnError<T>> for Error {
    fn from(err: ::rotor::SpawnError<T>) -> Self {
        match err {
            ::rotor::SpawnError::NoSlabSpace(_) => {
                error!("out of memory");

                ErrorKind::OutOfMemory.into()
            }
            ::rotor::SpawnError::UserError(err) => {
                error!("spawn error, {}", err);

                ErrorKind::SpawnError.into()
            }
        }
    }
}
