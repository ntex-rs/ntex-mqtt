use slab::Slab;
use std::{
    collections::HashMap,
    fmt::{self, Write},
    io,
    iter::{IntoIterator, Iterator},
    ops::{Deref, DerefMut, Div, DivAssign},
    str::FromStr,
};

use error::MqttError;

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
    MultiWildcard,  // Multi-level wildcard #
}

unsafe impl Send for Level {}
unsafe impl Sync for Level {}

impl Level {
    pub fn parse<T: AsRef<str>>(s: T) -> Result<Level, MqttError> {
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

    #[inline]
    pub fn value(&self) -> Option<&str> {
        match *self {
            Level::Normal(ref s) | Level::Metadata(ref s) => Some(s),
            _ => None,
        }
    }

    #[inline]
    pub fn is_normal(&self) -> bool {
        if let Level::Normal(_) = *self {
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_metadata(&self) -> bool {
        if let Level::Metadata(_) = *self {
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        match *self {
            Level::Normal(ref s) => {
                s.chars().nth(0) != Some('$') && !s.contains(|c| c == '+' || c == '#')
            }
            Level::Metadata(ref s) => {
                s.chars().nth(0) == Some('$') && !s.contains(|c| c == '+' || c == '#')
            }
            _ => true,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct Topic(Vec<Level>);

unsafe impl Send for Topic {}
unsafe impl Sync for Topic {}

impl Topic {
    #[inline]
    pub fn levels(&self) -> &Vec<Level> {
        &self.0
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.0
            .iter()
            .position(|level| !level.is_valid())
            .or_else(|| {
                self.0
                    .iter()
                    .enumerate()
                    .position(|(pos, level)| match *level {
                        Level::MultiWildcard => pos != self.0.len() - 1,
                        Level::Metadata(_) => pos != 0,
                        _ => false,
                    })
            }).is_none()
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

impl Deref for Topic {
    type Target = Vec<Level>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Topic {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[macro_export]
macro_rules! topic {
    ($s:expr) => {
        $s.parse::<Topic>().unwrap()
    };
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
            Level::Normal(ref lhs) => !is_metadata(self) && lhs == self.as_ref(),
            Level::Metadata(ref lhs) => is_metadata(self) && lhs == self.as_ref(),
            Level::Blank => self.as_ref().is_empty(),
            Level::SingleWildcard | Level::MultiWildcard => !is_metadata(self),
        }
    }
}

macro_rules! match_topic {
    ($topic:expr, $levels:expr) => {{
        let mut lhs = $topic.0.iter();

        for rhs in $levels {
            match lhs.next() {
                Some(&Level::SingleWildcard) => {
                    if !rhs.match_level(&Level::SingleWildcard) {
                        break;
                    }
                }
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
    }};
}

pub trait MatchTopic {
    fn match_topic(&self, topic: &Topic) -> bool;
}

impl MatchTopic for Topic {
    fn match_topic(&self, topic: &Topic) -> bool {
        match_topic!(topic, &self.0)
    }
}

impl<T: AsRef<str>> MatchTopic for T {
    fn match_topic(&self, topic: &Topic) -> bool {
        match_topic!(topic, self.as_ref().split('/'))
    }
}

impl FromStr for Level {
    type Err = MqttError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, MqttError> {
        match s {
            "+" => Ok(Level::SingleWildcard),
            "#" => Ok(Level::MultiWildcard),
            "" => Ok(Level::Blank),
            _ => {
                if s.contains(|c| c == '+' || c == '#') {
                    Err(MqttError::InvalidTopic)
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
    type Err = MqttError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, MqttError> {
        s.split('/')
            .map(|level| Level::from_str(level))
            .collect::<Result<Vec<_>, MqttError>>()
            .map(Topic)
            .and_then(|topic| {
                if topic.is_valid() {
                    Ok(topic)
                } else {
                    Err(MqttError::InvalidTopic)
                }
            })
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Level::Normal(ref s) | Level::Metadata(ref s) => f.write_str(s.as_str()),
            Level::Blank => Ok(()),
            Level::SingleWildcard => f.write_char('+'),
            Level::MultiWildcard => f.write_char('#'),
        }
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut first = true;

        for level in &self.0 {
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
            Level::Normal(ref s) | Level::Metadata(ref s) => self.write(s.as_str().as_bytes()),
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

type TopicIdx = usize;
type StateIdx = usize;

#[derive(Debug, Eq, PartialEq, Clone, Default)]
struct State {
    next: HashMap<Level, StateIdx>,
    out: Option<TopicIdx>,
    single_wildcard: Option<StateIdx>,
    multi_wildcard: Option<TopicIdx>,
}

#[derive(Debug)]
pub struct TopicTree {
    topics: Slab<Topic>,
    states: Slab<State>,
    root: StateIdx,
}

impl TopicTree {
    pub fn new() -> TopicTree {
        let mut states = Slab::with_capacity(64);
        let root = states.insert(Default::default());

        TopicTree {
            topics: Slab::with_capacity(64),
            states: states,
            root: root,
        }
    }

    pub fn build<I: IntoIterator<Item = Topic>>(topics: I) -> TopicTree {
        let mut tree = TopicTree::new();

        for topic in topics {
            tree.add(&topic);
        }

        tree
    }

    pub fn add(&mut self, topic: &Topic) {
        let mut cur_state = self.root;
        let topic_idx = self
            .topics
            .iter()
            .find(|&(idx, t)| *t == *topic)
            .map(|(idx, t)| idx)
            .unwrap_or_else(|| self.topics.insert(topic.clone()));

        for level in &topic.0 {
            match *level {
                Level::Normal(_) | Level::Metadata(_) | Level::Blank => {
                    match self.states[cur_state].next.get(level) {
                        Some(&next_state) => cur_state = next_state,
                        None => {
                            let next_state = self.add_state();

                            self.states[cur_state]
                                .next
                                .insert(level.clone(), next_state);

                            cur_state = next_state;
                        }
                    };
                }
                Level::SingleWildcard => match self.states[cur_state].single_wildcard {
                    Some(next_state) => cur_state = next_state,
                    None => {
                        let next_state = self.add_state();

                        self.states[cur_state].single_wildcard = Some(next_state);

                        cur_state = next_state;
                    }
                },
                Level::MultiWildcard => {
                    if self.states[cur_state].multi_wildcard.is_none() {
                        self.states[cur_state].multi_wildcard = Some(topic_idx);
                    }

                    return;
                }
            }
        }

        self.states[cur_state].out = Some(topic_idx);
    }

    #[inline]
    fn add_state(&mut self) -> StateIdx {
        self.states.insert(Default::default())
    }

    pub fn match_topic(&self, topic: &Topic) -> Option<Vec<&Topic>> {
        let mut topics = Vec::with_capacity(16);

        self.match_state(&self.states[self.root], topic.0.as_slice(), &mut topics);

        if topics.is_empty() {
            None
        } else {
            Some(topics.iter().map(|&idx| &self.topics[idx]).collect())
        }
    }

    fn match_state(&self, state: &State, levels: &[Level], topics: &mut Vec<TopicIdx>) {
        if let Some(topic) = state.multi_wildcard {
            if let Some(&Level::Metadata(_)) = levels.first() {
                // skip it
            } else {
                topics.push(topic);
            }
        }

        match levels.split_first() {
            Some((level, levels)) => {
                if let Some(&next_state) = state.next.get(level) {
                    self.match_state(&self.states[next_state], levels, topics)
                }

                if let Some(next_state) = state.single_wildcard {
                    if !level.is_metadata() {
                        self.match_state(&self.states[next_state], levels, topics);
                    }
                }
            }
            None => {
                if let Some(topic) = state.out {
                    topics.push(topic);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;

    use super::*;

    #[test]
    fn test_level() {
        assert!(Level::normal("sport").is_normal());
        assert!(Level::metadata("$SYS").is_metadata());

        assert_eq!(Level::normal("sport").value(), Some("sport"));
        assert_eq!(Level::metadata("$SYS").value(), Some("$SYS"));

        assert_eq!(Level::normal("sport"), "sport".parse().unwrap());
        assert_eq!(Level::metadata("$SYS"), "$SYS".parse().unwrap());

        assert!(Level::Normal(String::from("sport")).is_valid());
        assert!(Level::Metadata(String::from("$SYS")).is_valid());

        assert!(!Level::Normal(String::from("$sport")).is_valid());
        assert!(!Level::Metadata(String::from("SYS")).is_valid());

        assert!(!Level::Normal(String::from("sport#")).is_valid());
        assert!(!Level::Metadata(String::from("SYS+")).is_valid());
    }

    #[test]
    fn test_valid_topic() {
        assert!(
            Topic(vec![
                Level::normal("sport"),
                Level::normal("tennis"),
                Level::normal("player1")
            ]).is_valid()
        );

        assert!(
            Topic(vec![
                Level::normal("sport"),
                Level::normal("tennis"),
                Level::MultiWildcard
            ]).is_valid()
        );
        assert!(
            Topic(vec![
                Level::metadata("$SYS"),
                Level::normal("tennis"),
                Level::MultiWildcard
            ]).is_valid()
        );

        assert!(
            Topic(vec![
                Level::normal("sport"),
                Level::SingleWildcard,
                Level::normal("player1")
            ]).is_valid()
        );

        assert!(
            !Topic(vec![
                Level::normal("sport"),
                Level::MultiWildcard,
                Level::normal("player1")
            ]).is_valid()
        );
        assert!(
            !Topic(vec![
                Level::normal("sport"),
                Level::metadata("$SYS"),
                Level::normal("player1")
            ]).is_valid()
        );
    }

    #[test]
    fn test_parse_topic() {
        assert_eq!(
            topic!("sport/tennis/player1"),
            vec![
                Level::normal("sport"),
                Level::normal("tennis"),
                Level::normal("player1")
            ].into()
        );

        assert_eq!(topic!(""), Topic(vec![Level::Blank]));
        assert_eq!(
            topic!("/finance"),
            vec![Level::Blank, Level::normal("finance")].into()
        );

        assert_eq!(topic!("$SYS"), vec![Level::metadata("$SYS")].into());

        assert!("sport/$SYS".parse::<Topic>().is_err());
    }

    #[test]
    fn test_multi_wildcard_topic() {
        assert_eq!(
            topic!("sport/tennis/#"),
            vec![
                Level::normal("sport"),
                Level::normal("tennis"),
                Level::MultiWildcard
            ].into()
        );

        assert_eq!(topic!("#"), vec![Level::MultiWildcard].into());

        assert!("sport/tennis#".parse::<Topic>().is_err());
        assert!("sport/tennis/#/ranking".parse::<Topic>().is_err());
    }

    #[test]
    fn test_single_wildcard_topic() {
        assert_eq!(topic!("+"), vec![Level::SingleWildcard].into());

        assert_eq!(
            topic!("+/tennis/#"),
            vec![
                Level::SingleWildcard,
                Level::normal("tennis"),
                Level::MultiWildcard
            ].into()
        );

        assert_eq!(
            topic!("sport/+/player1"),
            vec![
                Level::normal("sport"),
                Level::SingleWildcard,
                Level::normal("player1")
            ].into()
        );

        assert!("sport+".parse::<Topic>().is_err());
    }

    #[test]
    fn test_write_topic() {
        let mut v = vec![];
        let t = vec![
            Level::SingleWildcard,
            Level::normal("tennis"),
            Level::MultiWildcard,
        ].into();

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
        assert_eq!(
            Level::normal("sport") / Level::normal("tennis") / Level::normal("player1"),
            "sport/tennis/player1".parse().unwrap()
        );
        assert_eq!(
            topic!("sport/tennis") / Level::normal("player1"),
            "sport/tennis/player1".parse().unwrap()
        );
        assert_eq!(
            Level::normal("sport") / topic!("tennis/player1"),
            "sport/tennis/player1".parse().unwrap()
        );
        assert_eq!(
            topic!("sport/tennis") / topic!("player1/ranking"),
            "sport/tennis/player1/ranking".parse().unwrap()
        );

        let mut t = topic!("sport/tennis");

        t /= Level::normal("player1");

        assert_eq!(t, "sport/tennis/player1".parse().unwrap());

        t /= topic!("ranking");

        assert_eq!(t, "sport/tennis/player1/ranking".parse().unwrap());
    }

    #[test]
    fn test_topic_tree() {
        let tree = TopicTree::build(vec![
            topic!("sport/tennis/+"),
            topic!("sport/tennis/player1"),
            topic!("sport/tennis/player1/#"),
            topic!("sport/#"),
            topic!("sport/+"),
            topic!("#"),
            topic!("+"),
            topic!("+/+"),
            topic!("/+"),
            topic!("$SYS/#"),
            topic!("$SYS/monitor/+"),
            topic!("+/monitor/Clients"),
        ]);

        assert_eq!(tree.topics.len(), 12);
        assert_eq!(tree.states.len(), 15);

        assert_eq!(
            tree.match_topic(&topic!("sport/tennis/player1")),
            Some(vec![
                &topic!("#"),
                &topic!("sport/#"),
                &topic!("sport/tennis/player1/#"),
                &topic!("sport/tennis/player1"),
                &topic!("sport/tennis/+")
            ])
        );
        assert_eq!(
            tree.match_topic(&topic!("sport/tennis/player1/ranking")),
            Some(vec![
                &topic!("#"),
                &topic!("sport/#"),
                &topic!("sport/tennis/player1/#")
            ])
        );
        assert_eq!(
            tree.match_topic(&topic!("sport/tennis/player1/score/wimbledo")),
            Some(vec![
                &topic!("#"),
                &topic!("sport/#"),
                &topic!("sport/tennis/player1/#")
            ])
        );
        assert_eq!(
            tree.match_topic(&topic!("sport")),
            Some(vec![&topic!("#"), &topic!("sport/#"), &topic!("+")])
        );
        assert_eq!(
            tree.match_topic(&topic!("sport/")),
            Some(vec![
                &topic!("#"),
                &topic!("sport/#"),
                &topic!("sport/+"),
                &topic!("+/+")
            ])
        );
        assert_eq!(
            tree.match_topic(&topic!("/finance")),
            Some(vec![&topic!("#"), &topic!("/+"), &topic!("+/+")])
        );

        assert_eq!(
            tree.match_topic(&topic!("$SYS/monitor/Clients")),
            Some(vec![&topic!("$SYS/#"), &topic!("$SYS/monitor/+")])
        );
        assert_eq!(
            tree.match_topic(&topic!("/monitor/Clients")),
            Some(vec![&topic!("#"), &topic!("+/monitor/Clients")])
        );
    }
}
