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
