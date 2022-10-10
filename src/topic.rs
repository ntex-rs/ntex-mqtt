use std::fmt::{self, Write};
use std::{convert::TryFrom, io, ops};

use ntex::util::ByteString;

#[deprecated = "Use TopicFilter instead"]
pub type Topic = TopicFilter;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TopicError {
    InvalidTopic,
    InvalidLevel,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Level {
    Normal(ByteString),
    Metadata(ByteString),
    Blank,
    SingleWildcard, // Single level wildcard +
    MultiWildcard,  // Multi-level wildcard #
}

impl Level {
    fn is_valid(&self) -> bool {
        match *self {
            Level::Normal(ref s) | Level::Metadata(ref s) => {
                !s.contains(|c| c == '+' || c == '#')
            }
            _ => true,
        }
    }
}

fn match_topic<T: MatchLevel, L: Iterator<Item = T>>(
    topic_filter: &TopicFilter,
    topic: L,
) -> bool {
    let mut topic_filter = topic_filter.0.iter();

    for (index, topic_level) in topic.enumerate() {
        match topic_filter.next() {
            Some(Level::SingleWildcard) => {
                if !topic_level.match_level(&Level::SingleWildcard, index) {
                    break;
                }
            }
            Some(Level::MultiWildcard) => {
                return topic_level.match_level(&Level::MultiWildcard, index);
            }
            Some(level) if topic_level.match_level(level, index) => continue,
            _ => return false,
        }
    }

    match topic_filter.next() {
        Some(&Level::MultiWildcard) => true,
        Some(_) => false,
        None => true,
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TopicFilter(Vec<Level>);

impl TopicFilter {
    pub fn levels(&self) -> &Vec<Level> {
        &self.0
    }

    pub fn is_valid(&self) -> bool {
        self.0
            .iter()
            .position(|level| !level.is_valid())
            .or_else(|| {
                self.0.iter().enumerate().position(|(pos, level)| match *level {
                    Level::MultiWildcard => pos != self.0.len() - 1,
                    Level::Metadata(_) => pos != 0,
                    _ => false,
                })
            })
            .is_none()
    }

    pub fn matches_filter(&self, topic: &TopicFilter) -> bool {
        match_topic(self, topic.0.iter())
    }

    pub fn matches_topic<S: AsRef<str> + ?Sized>(&self, topic: &S) -> bool {
        match_topic(self, topic.as_ref().split('/'))
    }

    #[deprecated = "Use matches_filter instead"]
    pub fn matches(&self, topic: &TopicFilter) -> bool {
        match_topic(self, topic.0.iter())
    }

    #[deprecated = "Use matches_topic instead"]
    pub fn matches_str<S: AsRef<str> + ?Sized>(&self, topic: &S) -> bool {
        match_topic(self, topic.as_ref().split('/'))
    }
}

impl<'a> TryFrom<&'a [Level]> for TopicFilter {
    type Error = TopicError;

    fn try_from(s: &[Level]) -> Result<Self, Self::Error> {
        let mut v = vec![];
        v.extend_from_slice(s);

        TopicFilter::try_from(v)
    }
}

impl TryFrom<Vec<Level>> for TopicFilter {
    type Error = TopicError;

    fn try_from(v: Vec<Level>) -> Result<Self, Self::Error> {
        let tf = TopicFilter(v);
        if tf.is_valid() {
            Ok(tf)
        } else {
            Err(TopicError::InvalidTopic)
        }
    }
}

impl From<TopicFilter> for Vec<Level> {
    fn from(t: TopicFilter) -> Self {
        t.0
    }
}

impl ops::Deref for TopicFilter {
    type Target = Vec<Level>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for TopicFilter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

trait MatchLevel {
    fn match_level(&self, level: &Level, index: usize) -> bool;
}

impl MatchLevel for Level {
    fn match_level(&self, level: &Level, index: usize) -> bool {
        match_level_impl(self, level, index)
    }
}

impl<'a> MatchLevel for &'a Level {
    fn match_level(&self, level: &Level, index: usize) -> bool {
        match_level_impl(self, level, index)
    }
}

fn match_level_impl(this: &Level, level: &Level, _index: usize) -> bool {
    match level {
        Level::Normal(rhs) => matches!(this, Level::Normal(lhs) if lhs == rhs),
        Level::Metadata(rhs) => matches!(this, Level::Metadata(lhs) if lhs == rhs),
        Level::Blank => *this == Level::Blank,
        Level::SingleWildcard => *this != Level::MultiWildcard,
        Level::MultiWildcard => true,
    }
}

impl<T: AsRef<str>> MatchLevel for T {
    fn match_level(&self, level: &Level, index: usize) -> bool {
        match level {
            Level::Normal(lhs) => lhs == self.as_ref(),
            Level::Metadata(ref lhs) => is_system(self) && lhs == self.as_ref(),
            Level::Blank => self.as_ref().is_empty(),
            Level::SingleWildcard | Level::MultiWildcard => !(index == 0 && is_system(self)),
        }
    }
}

impl TryFrom<ByteString> for TopicFilter {
    type Error = TopicError;

    fn try_from(value: ByteString) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(TopicError::InvalidTopic);
        }

        value
            .split('/')
            .enumerate()
            .map(|(idx, level)| match level {
                "+" => Ok(Level::SingleWildcard),
                "#" => Ok(Level::MultiWildcard),
                "" => Ok(Level::Blank),
                _ => {
                    if level.contains(|c| c == '+' || c == '#') {
                        Err(TopicError::InvalidLevel)
                    } else if idx == 0 && is_system(level) {
                        Ok(Level::Metadata(recover_bstr(&value, level)))
                    } else {
                        Ok(Level::Normal(recover_bstr(&value, level)))
                    }
                }
            })
            .collect::<Result<Vec<_>, TopicError>>()
            .map(TopicFilter)
            .and_then(
                |topic| {
                    if topic.is_valid() {
                        Ok(topic)
                    } else {
                        Err(TopicError::InvalidTopic)
                    }
                },
            )
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Level::Normal(s) | Level::Metadata(s) => f.write_str(s.as_str()),
            Level::Blank => Ok(()),
            Level::SingleWildcard => f.write_char('+'),
            Level::MultiWildcard => f.write_char('#'),
        }
    }
}

impl fmt::Display for TopicFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter();
        let mut level = iter.next().unwrap();
        loop {
            level.fmt(f)?;
            if let Some(l) = iter.next() {
                level = l;
                f.write_char('/')?;
            } else {
                break;
            }
        }
        Ok(())
    }
}

pub(crate) trait WriteTopicExt: io::Write {
    fn write_level(&mut self, level: &Level) -> io::Result<usize> {
        match *level {
            Level::Normal(ref s) | Level::Metadata(ref s) => self.write(s.as_str().as_bytes()),
            Level::Blank => Ok(0),
            Level::SingleWildcard => self.write(b"+"),
            Level::MultiWildcard => self.write(b"#"),
        }
    }

    fn write_topic(&mut self, topic: &TopicFilter) -> io::Result<usize> {
        let mut n = 0;
        let mut iter = topic.0.iter();
        let mut level = iter.next().unwrap();
        loop {
            n += self.write_level(level)?;
            if let Some(l) = iter.next() {
                level = l;
                n += self.write(b"/")?;
            } else {
                break;
            }
        }
        Ok(n)
    }
}

impl<W: io::Write + ?Sized> WriteTopicExt for W {}

fn is_system<T: AsRef<str>>(s: T) -> bool {
    s.as_ref().starts_with('$')
}

fn recover_bstr(superset: &ByteString, subset: &str) -> ByteString {
    unsafe {
        ByteString::from_bytes_unchecked(superset.as_bytes().slice_ref(subset.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    pub fn lvl_normal<T: AsRef<str>>(s: T) -> Level {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid normal level `{}` contains +|#", s.as_ref());
        }

        Level::Normal(s.as_ref().into())
    }

    pub fn lvl_sys<T: AsRef<str>>(s: T) -> Level {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid normal level `{}` contains +|#", s.as_ref());
        }

        if !s.as_ref().starts_with('$') {
            panic!("invalid metadata level `{}` not starts with $", s.as_ref())
        }

        Level::Metadata(s.as_ref().into())
    }

    pub fn topic(topic: &'static str) -> TopicFilter {
        TopicFilter::try_from(ByteString::from_static(topic)).unwrap()
    }

    #[test_case("level", Ok(vec![lvl_normal("level")]) ; "1")]
    #[test_case("level/+", Ok(vec![lvl_normal("level"), Level::SingleWildcard]) ; "2")]
    #[test_case("a//#", Ok(vec![lvl_normal("a"), Level::Blank, Level::MultiWildcard]) ; "3")]
    #[test_case("$a///#", Ok(vec![lvl_sys("$a"), Level::Blank, Level::Blank, Level::MultiWildcard]) ; "4")]
    #[test_case("$a/#/", Err(TopicError::InvalidTopic) ; "5")]
    #[test_case("a+b", Err(TopicError::InvalidLevel) ; "6")]
    #[test_case("a/+b", Err(TopicError::InvalidLevel) ; "7")]
    #[test_case("$a/$b/", Ok(vec![lvl_sys("$a"), lvl_normal("$b"), Level::Blank]) ; "8")]
    #[test_case("#/a", Err(TopicError::InvalidTopic) ; "10")]
    #[test_case("", Err(TopicError::InvalidTopic) ; "11")]
    #[test_case("/finance", Ok(vec![Level::Blank, lvl_normal("finance")]) ; "12")]
    #[test_case("finance/", Ok(vec![lvl_normal("finance"), Level::Blank]) ; "13")]
    fn parsing(input: &str, expected: Result<Vec<Level>, TopicError>) {
        let actual = TopicFilter::try_from(ByteString::from(input));
        assert_eq!(actual, expected.map(|levels| TopicFilter(levels)));
    }

    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), lvl_normal("player1")], true; "1")]
    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), Level::MultiWildcard], true; "2")]
    #[test_case(vec![lvl_sys("$SYS"), lvl_normal("tennis"), lvl_normal("player1")], true; "3")]
    #[test_case(vec![lvl_normal("sport"), Level::SingleWildcard, lvl_normal("player1")], true; "4")]
    #[test_case(vec![lvl_normal("sport"), Level::MultiWildcard, lvl_normal("player1")], false; "5")]
    #[test_case(vec![lvl_normal("sport"), lvl_sys("$SYS"), lvl_normal("player1")], false; "6")]
    fn topic_is_valid(levels: Vec<Level>, expected: bool) {
        assert_eq!(expected, TopicFilter::try_from(levels).is_ok());
    }

    #[test]
    fn test_multi_wildcard_topic() {
        assert!(topic("sport/tennis/#").matches_filter(&TopicFilter(vec![
            lvl_normal("sport"),
            lvl_normal("tennis"),
            Level::MultiWildcard
        ])));

        assert!(topic("sport/tennis/#").matches_topic("sport/tennis"));

        assert!(topic("#").matches_filter(&TopicFilter(vec![Level::MultiWildcard])));
    }

    #[test]
    fn test_single_wildcard_topic() {
        assert!(topic("+")
            .matches_filter(&TopicFilter::try_from(vec![Level::SingleWildcard]).unwrap()));

        assert!(topic("+/tennis/#").matches_filter(&TopicFilter(vec![
            Level::SingleWildcard,
            lvl_normal("tennis"),
            Level::MultiWildcard
        ])));

        assert!(topic("sport/+/player1").matches_filter(&TopicFilter(vec![
            lvl_normal("sport"),
            Level::SingleWildcard,
            lvl_normal("player1")
        ])));
    }

    #[test]
    fn test_write_topic() {
        let mut v = vec![];
        let t = TopicFilter(vec![
            Level::SingleWildcard,
            lvl_normal("tennis"),
            Level::MultiWildcard,
        ]);

        assert_eq!(v.write_topic(&t).unwrap(), 10);
        assert_eq!(v, b"+/tennis/#");

        assert_eq!(format!("{}", t), "+/tennis/#");
        assert_eq!(t.to_string(), "+/tennis/#");
    }

    #[test]
    fn test_matches() {
        assert!("test".match_level(&Level::Normal("test".into()), 0));
        assert!("$SYS".match_level(&Level::Metadata("$SYS".into()), 0));

        let t: TopicFilter = topic("sport/tennis/player1/#");

        assert!(t.matches_topic("sport/tennis/player1"));
        assert!(t.matches_topic("sport/tennis/player1/ranking"));
        assert!(t.matches_topic("sport/tennis/player1/score/wimbledon"));

        assert!(topic("sport/#").matches_topic("sport"));

        let t: TopicFilter = topic("sport/tennis/+");

        assert!(t.matches_topic("sport/tennis/player1"));
        assert!(t.matches_topic("sport/tennis/player2"));
        assert!(!t.matches_topic("sport/tennis/player1/ranking"));

        let t: TopicFilter = topic("sport/+");

        assert!(!t.matches_topic("sport"));
        assert!(t.matches_topic("sport/"));

        assert!(topic("+/+").matches_topic("/finance"));
        assert!(topic("/+").matches_topic("/finance"));
        assert!(!topic("+").matches_topic("/finance"));

        assert!(!topic("#").matches_topic("$SYS"));
        assert!(!topic("+/monitor/Clients").matches_topic("$SYS/monitor/Clients"));
        assert!(topic(&"$SYS/#").matches_topic("$SYS/"));
        assert!(topic("$SYS/monitor/+").matches_topic("$SYS/monitor/Clients"));
    }
}
