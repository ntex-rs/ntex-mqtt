use std::io;
use std::ops::{Div, DivAssign};
use std::iter::Iterator;
use std::fmt::{self, Display, Formatter, Write};
use std::str::FromStr;
use std::convert::{AsRef, Into};

use error::*;
use error::ErrorKind::*;

#[inline]
fn is_metadata<T: AsRef<str>>(s: T) -> bool {
    s.as_ref().chars().nth(0) == Some('$')
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
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

    pub fn is_normal(&self) -> bool {
        if let Level::Normal(_) = *self {
            true
        } else {
            false
        }
    }

    pub fn is_metadata(&self) -> bool {
        if let Level::Metadata(_) = *self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct Topic(Vec<Level>);

impl Topic {
    pub fn levels(&self) -> &Vec<Level> {
        &self.0
    }
}

impl<'a> From<&'a [Level]> for Topic {
    fn from(s: &[Level]) -> Self {
        let mut v = vec![];

        v.extend_from_slice(s);

        Topic(v)
    }
}

impl From<Vec<Level>> for Topic {
    fn from(v: Vec<Level>) -> Self {
        Topic(v)
    }
}

impl Into<Vec<Level>> for Topic {
    fn into(self) -> Vec<Level> {
        self.0
    }
}

pub trait MatchLevel {
    fn match_level(&self, level: &Level) -> bool;
}

impl MatchLevel for Level {
    fn match_level(&self, level: &Level) -> bool {
        match *level {
            Level::Normal(ref lhs) => {
                if let Level::Normal(ref rhs) = *self {
                    lhs == rhs
                } else {
                    false
                }
            }
            Level::Metadata(ref lhs) => {
                if let Level::Metadata(ref rhs) = *self {
                    lhs == rhs
                } else {
                    false
                }
            }
            Level::Blank => *self == *self,
            Level::SingleWildcard | Level::MultiWildcard => !self.is_metadata(),
        }
    }
}

impl<T: AsRef<str>> MatchLevel for T {
    fn match_level(&self, level: &Level) -> bool {
        match *level {
            Level::Normal(ref lhs) => {
                if is_metadata(self) {
                    false
                } else {
                    lhs == self.as_ref()
                }
            }
            Level::Metadata(ref lhs) => {
                if is_metadata(self) {
                    lhs == self.as_ref()
                } else {
                    false
                }
            }
            Level::Blank => self.as_ref().is_empty(),
            Level::SingleWildcard | Level::MultiWildcard => !is_metadata(self),
        }
    }
}

macro_rules! match_topic {
    ($topic:expr, $levels:expr) => ({
        let mut lhs = $topic.0.iter();

        for rhs in $levels {
            match lhs.next() {
                Some(&Level::SingleWildcard) => {
                    if !rhs.match_level(&Level::SingleWildcard) {
                        break
                    }
                },
                Some(&Level::MultiWildcard) => {
                    return rhs.match_level(&Level::MultiWildcard);
                }
                Some(level) if rhs.match_level(level) => continue,
                _ => return false,
            }
        }

        match lhs.next() {
            Some(&Level::MultiWildcard) => true,
            Some(_) => false,
            None => true,
        }
    })
}

pub trait MatchTopic {
    fn match_topic(&self, topic: &Topic) -> bool;
}

impl MatchTopic for Topic {
    fn match_topic(&self, topic: &Topic) -> bool {
        match_topic!(topic, self.0.iter())
    }
}

impl<T: AsRef<str>> MatchTopic for T {
    fn match_topic(&self, topic: &Topic) -> bool {
        match_topic!(topic, self.as_ref().split('/'))
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
                } else if is_metadata(s) {
                    Ok(Level::Metadata(String::from(s)))
                } else {
                    Ok(Level::Normal(String::from(s)))
                }
            }
        }
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

impl Display for Level {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Level::Normal(ref s) => f.write_str(s.as_str()),
            Level::Metadata(ref s) => f.write_str(s.as_str()),
            Level::Blank => Ok(()),
            Level::SingleWildcard => f.write_char('+'),
            Level::MultiWildcard => f.write_char('#'),
        }
    }
}

impl Display for Topic {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut first = true;

        for level in self.0.iter() {
            if first {
                first = false;
            } else {
                f.write_char('/')?;
            }

            level.fmt(f)?;
        }

        Ok(())
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

impl Div<Level> for Level {
    type Output = Topic;

    fn div(self, rhs: Level) -> Topic {
        Topic(vec![self, rhs])
    }
}

impl Div<Topic> for Level {
    type Output = Topic;

    fn div(self, rhs: Topic) -> Topic {
        let mut v = vec![self];
        v.append(&mut rhs.into());
        Topic(v)
    }
}

impl Div<Level> for Topic {
    type Output = Topic;

    fn div(self, rhs: Level) -> Topic {
        let mut v: Vec<Level> = self.into();
        v.push(rhs);
        Topic(v)
    }
}

impl Div<Topic> for Topic {
    type Output = Topic;

    fn div(self, rhs: Topic) -> Topic {
        let mut v: Vec<Level> = self.into();
        v.append(&mut rhs.into());
        Topic(v)
    }
}

impl DivAssign<Level> for Topic {
    fn div_assign(&mut self, rhs: Level) {
        self.0.push(rhs)
    }
}

impl DivAssign<Topic> for Topic {
    fn div_assign(&mut self, rhs: Topic) {
        self.0.append(&mut rhs.into())
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;

    macro_rules! topic {
        ($s:expr) => ($s.parse::<Topic>().unwrap());
    }

    #[test]
    fn test_parse_topic() {
        assert_eq!(topic!("sport/tennis/player1"),
                   vec![Level::normal("sport"), Level::normal("tennis"), Level::normal("player1")]
                       .into());

        assert_eq!(topic!(""), Topic(vec![Level::Blank]));
        assert_eq!(topic!("/finance"),
                   vec![Level::Blank, Level::normal("finance")].into());

        assert_eq!(topic!("$SYS"), vec![Level::metadata("$SYS")].into());
    }

    #[test]
    fn test_multi_wildcard_topic() {
        assert_eq!(topic!("sport/tennis/#"),
                   vec![Level::normal("sport"), Level::normal("tennis"), Level::MultiWildcard]
                       .into());

        assert_eq!(topic!("#"), vec![Level::MultiWildcard].into());

        assert!("sport/tennis#".parse::<Topic>().is_err());
        assert!("sport/tennis/#/ranking".parse::<Topic>().is_err());
    }

    #[test]
    fn test_single_wildcard_topic() {
        assert_eq!(topic!("+"), vec![Level::SingleWildcard].into());

        assert_eq!(topic!("+/tennis/#"),
                   vec![Level::SingleWildcard, Level::normal("tennis"), Level::MultiWildcard]
                       .into());

        assert_eq!(topic!("sport/+/player1"),
                   vec![Level::normal("sport"), Level::SingleWildcard, Level::normal("player1")]
                       .into());

        assert!("sport+".parse::<Topic>().is_err());
    }

    #[test]
    fn test_write_topic() {
        let mut v = vec![];
        let t = vec![Level::SingleWildcard, Level::normal("tennis"), Level::MultiWildcard].into();

        assert_eq!(v.write_topic(&t).unwrap(), 10);
        assert_eq!(v, b"+/tennis/#");

        assert_eq!(format!("{}", t), "+/tennis/#");
        assert_eq!(t.to_string(), "+/tennis/#");
    }

    #[test]
    fn test_match_topic() {
        assert!("test".match_level(&Level::normal("test")));
        assert!("$SYS".match_level(&Level::metadata("$SYS")));

        let t = "sport/tennis/player1/#".parse().unwrap();

        assert!("sport/tennis/player1".match_topic(&t));
        assert!("sport/tennis/player1/ranking".match_topic(&t));
        assert!("sport/tennis/player1/score/wimbledon".match_topic(&t));

        assert!("sport".match_topic(&"sport/#".parse().unwrap()));

        let t = "sport/tennis/+".parse().unwrap();

        assert!("sport/tennis/player1".match_topic(&t));
        assert!("sport/tennis/player2".match_topic(&t));
        assert!(!"sport/tennis/player1/ranking".match_topic(&t));

        let t = "sport/+".parse().unwrap();

        assert!(!"sport".match_topic(&t));
        assert!("sport/".match_topic(&t));

        assert!("/finance".match_topic(&"+/+".parse().unwrap()));
        assert!("/finance".match_topic(&"/+".parse().unwrap()));
        assert!(!"/finance".match_topic(&"+".parse().unwrap()));

        assert!(!"$SYS".match_topic(&"#".parse().unwrap()));
        assert!(!"$SYS/monitor/Clients".match_topic(&"+/monitor/Clients".parse().unwrap()));
        assert!("$SYS/".match_topic(&"$SYS/#".parse().unwrap()));
        assert!("$SYS/monitor/Clients".match_topic(&"$SYS/monitor/+".parse().unwrap()));
    }

    #[test]
    fn test_operators() {
        assert_eq!(Level::normal("sport") / Level::normal("tennis") / Level::normal("player1"),
                   "sport/tennis/player1".parse().unwrap());
        assert_eq!(topic!("sport/tennis") / Level::normal("player1"),
                   "sport/tennis/player1".parse().unwrap());
        assert_eq!(Level::normal("sport") / topic!("tennis/player1"),
                   "sport/tennis/player1".parse().unwrap());
        assert_eq!(topic!("sport/tennis") / topic!("player1/ranking"),
                   "sport/tennis/player1/ranking".parse().unwrap());

        let mut t = topic!("sport/tennis");

        t /= Level::normal("player1");

        assert_eq!(t, "sport/tennis/player1".parse().unwrap());

        t /= topic!("ranking");

        assert_eq!(t, "sport/tennis/player1/ranking".parse().unwrap());
    }
}
