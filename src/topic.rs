use std::io;
use std::str::FromStr;
use std::string::ToString;
use std::convert::AsRef;

use itertools::Itertools;

use error::*;
use error::ErrorKind::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Level {
    Normal(String),
    Metadata(String), // $SYS
    Blank,
    SingleWildcard, // Single level wildcard +
    MultiWildcard, // Multi-level wildcard #
}

impl Level {
    pub fn parse<T: AsRef<str>>(s: T) -> Result<Level> {
        Level::from_str(s.as_ref())
    }

    pub fn normal<T: AsRef<str>>(s: T) -> Level {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid normal level `{}` contains +|#", s.as_ref());
        }

        if s.as_ref().chars().nth(0) == Some('$') {
            panic!("invalid normal level `{}` starts with $", s.as_ref())
        }

        Level::Normal(String::from(s.as_ref()))
    }

    pub fn metadata<T: AsRef<str>>(s: T) -> Level {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid metadata level `{}` contains +|#", s.as_ref());
        }

        if s.as_ref().chars().nth(0) != Some('$') {
            panic!("invalid metadata level `{}` not starts with $", s.as_ref())
        }

        Level::Metadata(String::from(s.as_ref()))
    }
}

impl FromStr for Level {
    type Err = Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "+" => Ok(Level::SingleWildcard),
            "#" => Ok(Level::MultiWildcard),
            "" => Ok(Level::Blank),
            _ => {
                if s.contains(|c| c == '+' || c == '#') {
                    debug!("invalid level `{}` contains +|#", s);

                    bail!(InvalidTopic)
                } else if s.chars().nth(0) == Some('$') {
                    Ok(Level::Metadata(String::from(s)))
                } else {
                    Ok(Level::Normal(String::from(s)))
                }
            }
        }
    }
}

impl ToString for Level {
    #[inline]
    fn to_string(&self) -> String {
        match *self {
            Level::Normal(ref s) => s.clone(),
            Level::Metadata(ref s) => s.clone(),
            Level::Blank => String::from(""),
            Level::SingleWildcard => String::from("+"),
            Level::MultiWildcard => String::from("#"),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Topic(Vec<Level>);

impl Topic {
    pub fn levels(&self) -> &Vec<Level> {
        &self.0
    }

    pub fn from_slice(s: &[Level]) -> Topic {
        let mut v = vec![];

        v.extend_from_slice(s);

        Topic(v)
    }

    #[inline]
    pub fn parse<T: AsRef<str>>(s: T) -> Result<Topic> {
        Topic::from_str(s.as_ref())
    }
}

impl FromStr for Topic {
    type Err = Error;

    #[inline]
    fn from_str(s: &str) -> Result<Self> {
        debug!("parse topic `{}`", s);

        s.split('/')
            .map(|level| Level::from_str(level))
            .collect::<Result<Vec<_>>>()
            .and_then(|levels| {
                match levels.iter().position(|level| match *level {
                    Level::MultiWildcard => true,
                    _ => false,
                }) {
                    Some(pos) if pos != levels.len() - 1 => {
                        debug!("invalid topic `{}` contains # at level {}",
                               s,
                               levels.len() - pos);

                        bail!(InvalidTopic)
                    }
                    _ => Ok(levels),
                }
            })
            .map(|levels| Topic(levels))
    }
}

impl ToString for Topic {
    #[inline]
    fn to_string(&self) -> String {
        self.0
            .iter()
            .map(Level::to_string)
            .join("/")
    }
}

pub trait WriteTopicExt: io::Write {
    fn write_level(&mut self, level: &Level) -> io::Result<usize> {
        match *level {
            Level::Normal(ref s) => self.write(s.as_str().as_bytes()),
            Level::Metadata(ref s) => self.write(s.as_str().as_bytes()),
            Level::Blank => Ok(0),
            Level::SingleWildcard => self.write(b"+"),
            Level::MultiWildcard => self.write(b"#"),
        }
    }

    fn write_topic(&mut self, topic: &Topic) -> io::Result<usize> {
        let mut n = 0;
        let mut first = true;

        for level in topic.levels() {
            if first {
                first = false;
            } else {
                n += self.write(b"/")?;
            }

            n += self.write_level(level)?;
        }

        Ok(n)
    }
}

impl<W: io::Write + ?Sized> WriteTopicExt for W {}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;

    #[test]
    fn test_parse_topic() {
        assert_eq!(Topic::parse("sport/tennis/player1").unwrap(),
                   Topic(vec![Level::normal("sport"),
                              Level::normal("tennis"),
                              Level::normal("player1")]));

        assert_eq!(Topic::parse("").unwrap(), Topic(vec![Level::Blank]));
        assert_eq!(Topic::parse("/finance").unwrap(),
                   Topic(vec![Level::Blank, Level::normal("finance")]));

        assert_eq!(Topic::parse("$SYS").unwrap(),
                   Topic(vec![Level::metadata("$SYS")]));
    }

    #[test]
    fn test_multi_wildcard_topic() {
        assert_eq!(Topic::parse("sport/tennis/#").unwrap(),
                   Topic(vec![Level::normal("sport"),
                              Level::normal("tennis"),
                              Level::MultiWildcard]));

        assert_eq!(Topic::parse("#").unwrap(),
                   Topic(vec![Level::MultiWildcard]));

        assert!(Topic::parse("sport/tennis#").is_err());
        assert!(Topic::parse("sport/tennis/#/ranking").is_err());
    }

    #[test]
    fn test_single_wildcard_topic() {
        assert_eq!(Topic::parse("+").unwrap(),
                   Topic(vec![Level::SingleWildcard]));

        assert_eq!(Topic::parse("+/tennis/#").unwrap(),
                   Topic(vec![Level::SingleWildcard,
                              Level::normal("tennis"),
                              Level::MultiWildcard]));

        assert_eq!(Topic::parse("sport/+/player1").unwrap(),
                   Topic(vec![Level::normal("sport"),
                              Level::SingleWildcard,
                              Level::normal("player1")]));

        assert!(Topic::parse("sport+").is_err());
    }

    #[test]
    fn test_write_topic() {
        let mut v = vec![];
        let t = Topic(vec![Level::SingleWildcard, Level::normal("tennis"), Level::MultiWildcard]);

        assert_eq!(v.write_topic(&t).unwrap(), 10);
        assert_eq!(v, b"+/tennis/#");
    }
}
