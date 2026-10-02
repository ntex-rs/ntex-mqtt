use std::{fmt, fmt::Write, io};

use ntex_bytes::ByteString;

use crate::error::SpecViolation;

#[allow(clippy::match_same_arms)]
pub(crate) fn is_valid(topic: &str) -> bool {
    if topic.is_empty() {
        false
    } else {
        enum PrevState {
            None,
            LevelSep,
            SingleWildcard,
            MultiWildcard,
            Other,
        }

        let mut previous = PrevState::None;
        for current in topic.bytes() {
            previous = match (current, &previous) {
                (_, PrevState::MultiWildcard) => return false, // `#` is not last char
                (b'+', PrevState::None | PrevState::LevelSep) => PrevState::SingleWildcard,
                (b'#', PrevState::None | PrevState::LevelSep) => PrevState::MultiWildcard,
                (b'+' | b'#', _) => return false, // `+` or `#` after char other than `/`
                (b'/', _) => PrevState::LevelSep,
                (_, PrevState::SingleWildcard) => return false, // `+` is followed by char other than `/`
                _ => PrevState::Other,
            }
        }
        true
    }
}

/// Shared Subscription Topic Filters start with `$share/` (MQTT 5.0, 4.8.2)
pub(crate) fn is_shared(filter: &str) -> bool {
    filter.starts_with("$share/")
}

/// The `ShareName` of a Shared Subscription must be at least one character long, must not
/// contain "/", "+" or "#" and must be followed by "/" and a Topic Filter,
/// [MQTT-4.8.2-1], [MQTT-4.8.2-2] (MQTT 5.0, 4.8.2)
pub(crate) fn is_valid_shared(filter: &str) -> bool {
    filter.strip_prefix("$share/").is_none_or(|rest| {
        rest.split_once('/').is_some_and(|(name, filter)| {
            !name.is_empty() && !name.contains(['+', '#']) && !filter.is_empty()
        })
    })
}

/// The Will Topic is a Topic Name, it must be at least one character long
/// and must not contain wildcards, [MQTT-4.7.3-1], [MQTT-4.7.0-1] (MQTT 5.0, 4.7),
/// [MQTT-4.7.3-1], [MQTT-4.7.1-1] (MQTT 3.1.1, 4.7)
pub(crate) fn check_will_topic(topic: &str) -> Result<(), SpecViolation> {
    if topic.is_empty() {
        Err(SpecViolation::Will_4_7_3_1)
    } else if topic.contains(['+', '#']) {
        Err(SpecViolation::Will_4_7_0_1)
    } else {
        Ok(())
    }
}

/// Errors which can occur when parsing a topic filter
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TopicFilterError {
    /// Topic filter is malformed
    InvalidTopic,
    /// Topic filter level is malformed
    InvalidLevel,
}

/// Single level of a topic filter
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TopicFilterLevel {
    /// Ordinary level name
    Normal(ByteString),
    /// Level name of a reserved topic, it starts with `$`
    System(ByteString),
    /// Empty level name
    Blank,
    /// Single level wildcard `+`
    SingleWildcard,
    /// Multi-level wildcard `#`
    MultiWildcard,
}

impl TopicFilterLevel {
    /// Checks the level at `pos` of `len` levels, it must be the level
    /// that parsing of the topic filter string produces
    fn is_valid(&self, pos: usize, len: usize) -> bool {
        match self {
            // a non-empty level name without separators or wildcards (MQTT 5.0, 4.7.1)
            // that starts with `$` only for a reserved topic (MQTT 5.0, 4.7.2)
            TopicFilterLevel::Normal(s) => {
                !s.is_empty() && !s.contains(['/', '+', '#']) && (pos != 0 || !is_system(s))
            }
            TopicFilterLevel::System(s) => pos == 0 && is_system(s) && !s.contains(['/', '+', '#']),
            TopicFilterLevel::Blank | TopicFilterLevel::SingleWildcard => true,
            // [MQTT-4.7.1-1] the multi-level wildcard must be the last level
            TopicFilterLevel::MultiWildcard => pos == len - 1,
        }
    }
}

fn match_topic<T: MatchLevel, L: Iterator<Item = T>>(superset: &TopicFilter, subset: L) -> bool {
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
            Some(level) if subset_level.match_level(level, index) => (),
            _ => return false,
        }
    }

    match superset.next() {
        Some(&TopicFilterLevel::MultiWildcard) | None => true,
        Some(_) => false,
    }
}

/// Parsed mqtt topic filter
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize)]
pub struct TopicFilter(Vec<TopicFilterLevel>);

impl<'de> serde::Deserialize<'de> for TopicFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // same format as the derived impl, the levels are validated
        #[derive(serde::Deserialize)]
        #[serde(rename = "TopicFilter")]
        struct Levels(Vec<TopicFilterLevel>);

        let Levels(levels) = Levels::deserialize(deserializer)?;
        TopicFilter::try_from(levels).map_err(|_| serde::de::Error::custom("invalid topic filter"))
    }
}

impl TopicFilter {
    /// Returns levels of the topic filter
    pub fn levels(&self) -> &[TopicFilterLevel] {
        &self.0
    }

    fn is_valid(&self) -> bool {
        // a topic filter has at least one level (MQTT 5.0, 4.7.3)
        !self.0.is_empty()
            && self
                .0
                .iter()
                .enumerate()
                .all(|(pos, level)| level.is_valid(pos, self.0.len()))
    }

    /// Check if the topic filter matches another topic filter
    pub fn matches_filter(&self, topic: &TopicFilter) -> bool {
        match_topic(self, topic.0.iter())
    }

    /// Check if the topic filter matches the topic name
    pub fn matches_topic<S: AsRef<str> + ?Sized>(&self, topic: &S) -> bool {
        match_topic(self, topic.as_ref().split('/'))
    }
}

impl TryFrom<&[TopicFilterLevel]> for TopicFilter {
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

impl MatchLevel for &TopicFilterLevel {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool {
        match_level_impl(self, level, index)
    }
}

fn match_level_impl(
    subset_level: &TopicFilterLevel,
    superset_level: &TopicFilterLevel,
    index: usize,
) -> bool {
    // a wildcard at the first level does not match a topic starting with `$`,
    // [MQTT-4.7.2-1] (MQTT 5.0, 4.7.2), (MQTT 3.1.1, 4.7.2)
    let is_sys = index == 0 && matches!(subset_level, TopicFilterLevel::System(_));
    match superset_level {
        TopicFilterLevel::Normal(rhs) => {
            matches!(subset_level, TopicFilterLevel::Normal(lhs) if lhs == rhs)
        }
        TopicFilterLevel::System(rhs) => {
            matches!(subset_level, TopicFilterLevel::System(lhs) if lhs == rhs)
        }
        TopicFilterLevel::Blank => *subset_level == TopicFilterLevel::Blank,
        TopicFilterLevel::SingleWildcard => {
            !is_sys && *subset_level != TopicFilterLevel::MultiWildcard
        }
        TopicFilterLevel::MultiWildcard => !is_sys,
    }
}

impl<T: AsRef<str>> MatchLevel for T {
    fn match_level(&self, level: &TopicFilterLevel, index: usize) -> bool {
        match level {
            TopicFilterLevel::Normal(lhs) => lhs == self.as_ref(),
            TopicFilterLevel::System(lhs) => is_system(self) && lhs == self.as_ref(),
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
                    if level.contains(['+', '#']) {
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
            TopicFilterLevel::Normal(s) | TopicFilterLevel::System(s) => f.write_str(s.as_str()),
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

#[allow(dead_code)]
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
    unsafe { ByteString::from_bytes_unchecked(superset.as_bytes().slice_ref(subset.as_bytes())) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case("abc" => true; "pass_norm1")]
    #[test_case("a/b" => true; "pass_norm2")]
    #[test_case("/" => true; "pass_norm3")]
    #[test_case("//" => true; "pass_norm4")]
    #[test_case("a/b/+" => true; "pass_plus1")]
    #[test_case("+/a" => true; "pass_plus2")]
    #[test_case("+" => true; "pass_plus3")]
    #[test_case("+//+" => true; "pass_plus4")]
    #[test_case("a/b/#" => true; "pass_hash1")]
    #[test_case("#" => true; "pass_hash2")]
    #[test_case("/#" => true; "pass_hash3")]
    #[test_case("++" => false; "fail_plus1")]
    #[test_case("b+/" => false; "fail_plus2")]
    #[test_case("a/+b" => false; "fail_plus3")]
    #[test_case("+#" => false; "fail_hash1")]
    #[test_case("a#" => false; "fail_hash2")]
    #[test_case("a/#/" => false; "fail_hash3")]
    #[test_case("a/#b" => false; "fail_hash4")]
    #[test_case("a/##" => false; "fail_hash5")]
    #[test_case("a/#+" => false; "fail_hash6")]
    fn check_is_valid(topic_filter: &'static str) -> bool {
        is_valid(topic_filter)
    }

    fn lvl_normal<T: AsRef<str>>(s: T) -> TopicFilterLevel {
        assert!(
            !s.as_ref().contains(['+', '#']),
            "invalid normal level `{}` contains +|#",
            s.as_ref()
        );
        TopicFilterLevel::Normal(s.as_ref().into())
    }

    fn lvl_sys<T: AsRef<str>>(s: T) -> TopicFilterLevel {
        assert!(
            !s.as_ref().contains(['+', '#']),
            "invalid normal level `{}` contains +|#",
            s.as_ref()
        );
        assert!(
            s.as_ref().starts_with('$'),
            "invalid metadata level `{}` not starts with $",
            s.as_ref()
        );
        TopicFilterLevel::System(s.as_ref().into())
    }

    fn topic(topic: &'static str) -> TopicFilter {
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
        TopicFilter::try_from(ByteString::from(input)).map(|t| t.levels().to_vec())
    }

    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), lvl_normal("player1")] => true; "1")]
    #[test_case(vec![lvl_normal("sport"), lvl_normal("tennis"), TopicFilterLevel::MultiWildcard] => true; "2")]
    #[test_case(vec![lvl_sys("$SYS"), lvl_normal("tennis"), lvl_normal("player1")] => true; "3")]
    #[test_case(vec![lvl_normal("sport"), TopicFilterLevel::SingleWildcard, lvl_normal("player1")] => true; "4")]
    #[test_case(vec![lvl_normal("sport"), TopicFilterLevel::MultiWildcard, lvl_normal("player1")] => false; "5")]
    #[test_case(vec![lvl_normal("sport"), lvl_sys("$SYS"), lvl_normal("player1")] => false; "6")]
    #[test_case(vec![] => false; "7")]
    #[test_case(vec![lvl_normal("a/b")] => false; "8")]
    #[test_case(vec![lvl_normal("a"), TopicFilterLevel::Normal("".into())] => false; "9")]
    #[test_case(vec![TopicFilterLevel::Normal("$SYS".into()), TopicFilterLevel::MultiWildcard] => false; "10")]
    #[test_case(vec![TopicFilterLevel::System("SYS".into())] => false; "11")]
    #[test_case(vec![TopicFilterLevel::System("$SYS/a".into())] => false; "12")]
    #[test_case(vec![TopicFilterLevel::System("$S+".into())] => false; "13")]
    #[test_case(vec![lvl_normal("a"), lvl_normal("$b"), TopicFilterLevel::Blank] => true; "14")]
    #[test_case(vec![TopicFilterLevel::Blank, TopicFilterLevel::SingleWildcard, TopicFilterLevel::MultiWildcard] => true; "15")]
    #[test_case(vec![TopicFilterLevel::MultiWildcard] => true; "16")]
    fn topic_is_valid(levels: Vec<TopicFilterLevel>) -> bool {
        TopicFilter::try_from(levels).is_ok()
    }

    #[test]
    fn test_empty_levels() {
        assert_eq!(
            TopicFilter::try_from(vec![]),
            Err(TopicFilterError::InvalidTopic)
        );
        assert_eq!(
            TopicFilter::try_from(&[][..]),
            Err(TopicFilterError::InvalidTopic)
        );
    }

    #[test]
    fn test_serde() {
        let tf = topic("$SYS/+/a//#");
        let json = serde_json::to_string(&tf).unwrap();
        let de: TopicFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(de, tf);
        assert_eq!(de.to_string(), "$SYS/+/a//#");

        for json in [
            "[]",
            r#"["MultiWildcard","SingleWildcard"]"#,
            r#"[{"Normal":"a"},{"System":"$b"}]"#,
            r#"[{"Normal":"a+"}]"#,
            r#"[{"Normal":"a/b"}]"#,
            r#"[{"System":"a"}]"#,
        ] {
            assert!(serde_json::from_str::<TopicFilter>(json).is_err(), "{json}");
        }
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

        assert_eq!(format!("{t}"), "+/tennis/#");
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
    #[test_case("+/a", "$SYS/a" => false; "sys1")]
    #[test_case("+", "$SYS" => false; "sys2")]
    #[test_case("#", "$SYS/a" => false; "sys3")]
    #[test_case("#", "$SYS/#" => false; "sys4")]
    #[test_case("+/#", "$SYS/+" => false; "sys5")]
    #[test_case("$SYS/#", "$SYS/+" => true; "sys6")]
    #[test_case("$SYS/+", "$SYS/a" => true; "sys7")]
    #[test_case("#", "a/$SYS" => true; "sys8")]
    #[test_case("+/+", "a/$SYS" => true; "sys9")]
    fn matches_filter(superset_filter: &'static str, subset_filter: &'static str) -> bool {
        topic(superset_filter).matches_filter(&topic(subset_filter))
    }

    #[test_case("a/b" => true; "not_shared")]
    #[test_case("$share" => true; "not_shared_no_sep")]
    #[test_case("$share/g/a" => true; "pass1")]
    #[test_case("$share/g/#" => true; "pass2")]
    #[test_case("$share/g/+/a/" => true; "pass3")]
    #[test_case("$share/group1//" => true; "pass4")]
    #[test_case("$share//a" => false; "fail_empty_name")]
    #[test_case("$share/g" => false; "fail_no_filter")]
    #[test_case("$share/g/" => false; "fail_empty_filter")]
    #[test_case("$share/+/a" => false; "fail_name_plus")]
    #[test_case("$share/#/a" => false; "fail_name_hash")]
    #[test_case("$share/a+b/c" => false; "fail_name_plus_mid")]
    fn is_valid_shared(filter: &str) -> bool {
        super::is_valid_shared(filter)
    }
}
