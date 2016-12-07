error_chain!{
    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error) #[cfg(unix)];
    }

    errors {
        InvalidState
        InvalidPacket
    }
}
