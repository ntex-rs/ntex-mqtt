use std::fmt::{self, Write};
use std::{convert::TryFrom, io};

use ntex::util::ByteString;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TopicFilterError {
    InvalidTopic,
    InvalidLevel,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TopicFilterLevel {
    Normal(ByteString),
    System(ByteString),
    Blank,
    SingleWildcard, // Single level wildcard +
    MultiWildcard,  // Multi-level wildcard #
}

impl TopicFilterLevel {
    fn is_valid(&self) -> bool {
        match *self {
            TopicFilterLevel::Normal(ref s) | TopicFilterLevel::System(ref s) => {
                !s.contains(|c| c == '+' || c == '#')
            }
            _ => true,
        }
    }
}

fn match_topic<T: MatchLevel, L: Iterator<Item = T>>(
    superset: &TopicFilter,
    subset: L,
) -> bool {
    let mut superset = superset.0.iter();

    for (index, subset_level) in subset.enumerate() {
        match superset.next() {
            Some(TopicFilterLevel::SingleWildcard) => {
                if !subset_level.match_level(&TopicFilterLevel::SingleWildcard, index) {
                    return false;
                }
            }
            Some(TopicFilterLevel::MultiWildcard) => {
                return subset_level.match_level(&TopicFilterLevel::MultiWildcard, index);
            }
            Some(level) if subset_level.match_level(level, index) => continue,
            _ => return false,
        }
    }

    match superset.next() {
        Some(&TopicFilterLevel::MultiWildcard) => true,
        Some(_) => false,
        None => true,
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TopicFilter(Vec<TopicFilterLevel>);

impl TopicFilter {
    pub fn levels(&self) -> &[TopicFilterLevel] {
        &self.0
    }

    fn is_valid(&self) -> bool {
        self.0
            .iter()
            .position(|level| !level.is_valid())
            .or_else(|| {
                self.0.iter().enumerate().position(|(pos, level)| match *level {
                    TopicFilterLevel::MultiWildcard => pos != self.0.len() - 1,
                    TopicFilterLevel::System(_) => pos != 0,
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
}

impl<'a> TryFrom<&'a [TopicFilterLevel]> for TopicFilter {
    type Error = TopicFilterError;

    fn try_from(s: &[TopicFilterLevel]) -> Result<Self, Self::Error> {
        let mut v = vec![];
        v.extend_from_slice(s);

        TopicFilter::try_from(v)
    }
}

impl TryFrom<Vec<TopicFilterLevel>> for TopicFilter {
    type Error = TopicFilterError;

    fn try_from(v: Vec<TopicFilterLevel>) -> Result<Self, Self::Error> {
        let tf = TopicFilter(v);
        if tf.is_valid() {
            Ok(tf)
        } else {
            Err(TopicFilterError::InvalidTopic)
        }
    }
}

impl From<TopicFilter> for Vec<TopicFilterLevel> {
    fn from(t: TopicFilter) -> Self {
        t.0
    }
}

trait MatchLevel {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool;
}

impl MatchLevel for TopicFilterLevel {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool {
        match_level_impl(self, level, index)
    }
}

impl<'a> MatchLevel for &'a TopicFilterLevel {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool {
        match_level_impl(self, level, index)
    }
}

fn match_level_impl(
    subset_level: &TopicFilterLevel,
    superset_level: &TopicFilterLevel,
    _index: usize,
) -> bool {
    match superset_level {
        TopicFilterLevel::Normal(rhs) => {
            matches!(subset_level, TopicFilterLevel::Normal(lhs) if lhs == rhs)
        }
        TopicFilterLevel::System(rhs) => {
            matches!(subset_level, TopicFilterLevel::System(lhs) if lhs == rhs)
        }
        TopicFilterLevel::Blank => *subset_level == TopicFilterLevel::Blank,
        TopicFilterLevel::SingleWildcard => *subset_level != TopicFilterLevel::MultiWildcard,
        TopicFilterLevel::MultiWildcard => true,
    }
}

impl<T: AsRef<str>> MatchLevel for T {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool {
        match level {
            TopicFilterLevel::Normal(lhs) => lhs == self.as_ref(),
            TopicFilterLevel::System(ref lhs) => is_system(self) && lhs == self.as_ref(),
            TopicFilterLevel::Blank => self.as_ref().is_empty(),
            TopicFilterLevel::SingleWildcard | TopicFilterLevel::MultiWildcard => {
                !(index == 0 && is_system(self))
            }
        }
    }
}

impl TryFrom<ByteString> for TopicFilter {
    type Error = TopicFilterError;

    fn try_from(value: ByteString) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(TopicFilterError::InvalidTopic);
        }

        value
            .split('/')
            .enumerate()
            .map(|(idx, level)| match level {
                "+" => Ok(TopicFilterLevel::SingleWildcard),
                "#" => Ok(TopicFilterLevel::MultiWildcard),
                "" => Ok(TopicFilterLevel::Blank),
                _ => {
                    if level.contains(|c| c == '+' || c == '#') {
                        Err(TopicFilterError::InvalidLevel)
                    } else if idx == 0 && is_system(level) {
                        Ok(TopicFilterLevel::System(recover_bstr(&value, level)))
                    } else {
                        Ok(TopicFilterLevel::Normal(recover_bstr(&value, level)))
                    }
                }
            })
            .collect::<Result<Vec<_>, TopicFilterError>>()
            .map(TopicFilter)
            .and_then(|topic| {
                if topic.is_valid() {
                    Ok(topic)
                } else {
                    Err(TopicFilterError::InvalidTopic)
                }
            })
    }
}

impl std::str::FromStr for TopicFilter {
    type Err = TopicFilterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let s: ByteString = value.into();
        TopicFilter::try_from(s)
    }
}

impl fmt::Display for TopicFilterLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicFilterLevel::Normal(s) | TopicFilterLevel::System(s) => {
                f.write_str(s.as_str())
            }
            TopicFilterLevel::Blank => Ok(()),
            TopicFilterLevel::SingleWildcard => f.write_char('+'),
            TopicFilterLevel::MultiWildcard => f.write_char('#'),
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
    fn write_level(&mut self, level: &TopicFilterLevel) -> io::Result<usize> {
        match *level {
            TopicFilterLevel::Normal(ref s) | TopicFilterLevel::System(ref s) => {
                self.write(s.as_str().as_bytes())
            }
            TopicFilterLevel::Blank => Ok(0),
            TopicFilterLevel::SingleWildcard => self.write(b"+"),
            TopicFilterLevel::MultiWildcard => self.write(b"#"),
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

    pub fn lvl_normal<T: AsRef<str>>(s: T) -> TopicFilterLevel {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid normal level `{}` contains +|#", s.as_ref());
        }

        TopicFilterLevel::Normal(s.as_ref().into())
    }

    pub fn lvl_sys<T: AsRef<str>>(s: T) -> TopicFilterLevel {
        if s.as_ref().contains(|c| c == '+' || c == '#') {
            panic!("invalid normal level `{}` contains +|#", s.as_ref());
        }

        if !s.as_ref().starts_with('$') {
            panic!("invalid metadata level `{}` not starts with $", s.as_ref())
        }

        TopicFilterLevel::System(s.as_ref().into())
    }

    pub fn topic(topic: &'static str) -> TopicFilter {
        TopicFilter::try_from(ByteString::from_static(topic)).unwrap()
    }

    #[test_case("level" => Ok(vec![lvl_normal("level")]) ; "1")]
    #[test_case("level/+" => Ok(vec![lvl_normal("level"), TopicFilterLevel::SingleWildcard]) ; "2")]
    #[test_case("a//#" => Ok(vec![lvl_normal("a"), TopicFilterLevel::Blank, TopicFilterLevel::MultiWildcard]) ; "3")]
    #[test_case("$a///#" => Ok(vec![lvl_sys("$a"), TopicFilterLevel::Blank, TopicFilterLevel::Blank, TopicFilterLevel::MultiWildcard]) ; "4")]
    #[test_case("$a/#/" => Err(TopicFilterError::InvalidTopic) ; "5")]
    #[test_case("a+b" => Err(TopicFilterError::InvalidLevel) ; "6")]
    #[test_case("a/+b" => Err(TopicFilterError::InvalidLevel) ; "7")]
    #[test_case("$a/$b/" => Ok(vec![lvl_sys("$a"), lvl_normal("$b"), TopicFilterLevel::Blank]) ; "8")]
    #[test_case("#/a" => Err(TopicFilterError::InvalidTopic) ; "10")]
    #[test_case("" => Err(TopicFilterError::InvalidTopic) ; "11")]
    #[test_case("/finance" => Ok(vec![TopicFilterLevel::Blank, lvl_normal("finance")]) ; "12")]
    #[test_case("finance/" => Ok(vec![lvl_normal("finance"), TopicFilterLevel::Blank]) ; "13")]
    fn parsing(input: &str) -> Result<Vec<TopicFilterLevel>, TopicFilterError> {
        TopicFilter::try_from(ByteString::from(input))
            .map(|t| t.levels().iter().cloned().collect())
    }

    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), lvl_normal("player1")] => true; "1")]
    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), TopicFilterLevel::MultiWildcard] => true; "2")]
    #[test_case(vec![lvl_sys("$SYS"), lvl_normal("tennis"), lvl_normal("player1")] => true; "3")]
    #[test_case(vec![lvl_normal("sport"), TopicFilterLevel::SingleWildcard, lvl_normal("player1")] => true; "4")]
    #[test_case(vec![lvl_normal("sport"), TopicFilterLevel::MultiWildcard, lvl_normal("player1")] => false; "5")]
    #[test_case(vec![lvl_normal("sport"), lvl_sys("$SYS"), lvl_normal("player1")] => false; "6")]
    fn topic_is_valid(levels: Vec<TopicFilterLevel>) -> bool {
        TopicFilter::try_from(levels).is_ok()
    }

    #[test]
    fn test_multi_wildcard_topic() {
        assert!(topic("sport/tennis/#").matches_filter(&TopicFilter(vec![
            lvl_normal("sport"),
            lvl_normal("tennis"),
            TopicFilterLevel::MultiWildcard
        ])));

        assert!(topic("sport/tennis/#").matches_topic("sport/tennis"));

        assert!(topic("#").matches_filter(&TopicFilter(vec![TopicFilterLevel::MultiWildcard])));
    }

    #[test]
    fn test_single_wildcard_topic() {
        assert!(topic("+").matches_filter(
            &TopicFilter::try_from(vec![TopicFilterLevel::SingleWildcard]).unwrap()
        ));

        assert!(topic("+/tennis/#").matches_filter(&TopicFilter(vec![
            TopicFilterLevel::SingleWildcard,
            lvl_normal("tennis"),
            TopicFilterLevel::MultiWildcard
        ])));

        assert!(topic("sport/+/player1").matches_filter(&TopicFilter(vec![
            lvl_normal("sport"),
            TopicFilterLevel::SingleWildcard,
            lvl_normal("player1")
        ])));
    }

    #[test]
    fn test_write_topic() {
        let mut v = vec![];
        let t = TopicFilter(vec![
            TopicFilterLevel::SingleWildcard,
            lvl_normal("tennis"),
            TopicFilterLevel::MultiWildcard,
        ]);

        assert_eq!(v.write_topic(&t).unwrap(), 10);
        assert_eq!(v, b"+/tennis/#");

        assert_eq!(format!("{}", t), "+/tennis/#");
        assert_eq!(t.to_string(), "+/tennis/#");
    }

    #[test_case("test", "test" => true)]
    #[test_case("$SYS", "$SYS" => true)]
    #[test_case("sport/tennis/player1/#", "sport/tennis/player1" => true)]
    #[test_case("sport/tennis/player1/#", "sport/tennis/player1/score" => true)]
    #[test_case("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon" => true)]
    #[test_case("sport/#", "sport" => true)]
    #[test_case("sport/tennis/+", "sport/tennis/player1" => true)]
    #[test_case("sport/tennis/+", "sport/tennis/player2" => true)]
    #[test_case("sport/tennis/+", "sport/tennis/player1/ranking" => false)]
    #[test_case("sport/+", "sport" => false; "single1")]
    #[test_case("sport/+", "sport/" => true; "single2")]
    #[test_case("+/+", "/finance" => true; "single3")]
    #[test_case("/+", "/finance" => true; "single4")]
    #[test_case("+", "/finance" => false; "single5")]
    #[test_case("#", "$SYS" => false; "sys1")]
    #[test_case("+/monitor/Clients", "$SYS/monitor/Clients" => false; "sys2")]
    #[test_case("$SYS/#", "$SYS/" => true; "sys3")]
    #[test_case("$SYS/monitor/+", "$SYS/monitor/Clients" => true; "sys4")]
    #[test_case("#", "/$SYS/monitor/Clients" => true; "sys5")]
    #[test_case("+", "$SYS" => false; "sys6")]
    #[test_case("+/#", "$SYS" => false; "sys7")]
    fn matches_topic(filter: &'static str, topic_str: &'static str) -> bool {
        topic(filter).matches_topic(topic_str)
    }

    #[test_case("a/b", "a/b" => true; "1")]
    #[test_case("a/b", "a/+" => false; "2")]
    #[test_case("a/b", "a/#" => false; "3")]
    #[test_case("a/+", "a/#" => false; "4")]
    #[test_case("a/+", "a/b" => true; "5")]
    #[test_case("+/+", "/" => true; "6")]
    #[test_case("+/+", "#" => false; "7")]
    #[test_case("+", "#" => false; "8")]
    #[test_case("#", "+" => true; "9")]
    #[test_case("#", "#" => true; "10")]
    #[test_case("a/#", "a/+/+" => true; "11")]
    #[test_case("a/+/normal/+", "a/$not_sys/normal/+" => true; "12")]
    #[test_case("a/+/#", "a/b" => true; "13")]
    fn matches_filter(superset_filter: &'static str, subset_filter: &'static str) -> bool {
        topic(superset_filter).matches_filter(&topic(subset_filter))
    }
}
