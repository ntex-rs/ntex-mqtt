macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err($fmt, $($arg)+);
        }
    };
}

macro_rules! prim_enum {
    (
        $( #[$enum_attr:meta] )*
        pub enum $name:ident {
            $(
                $( #[$enum_item_attr:meta] )*
                $var:ident=$val:expr
            ),+
        }) => {
        $( #[$enum_attr] )*
        #[repr(u8)]
        #[derive(Debug, Eq, PartialEq, Copy, Clone)]
        pub enum $name {
            $(
                $( #[$enum_item_attr] )*
                $var = $val
            ),+
        }
        impl std::convert::TryFrom<u8> for $name {
            type Error = $crate::error::DecodeError;
            fn try_from(v: u8) -> Result<Self, Self::Error> {
                match v {
                    $($val => Ok($name::$var)),+
                    ,_ => Err($crate::error::DecodeError::MalformedPacket)
                }
            }
        }
        impl From<$name> for u8 {
            fn from(v: $name) -> Self {
                unsafe { ::std::mem::transmute(v) }
            }
        }
    };
}
