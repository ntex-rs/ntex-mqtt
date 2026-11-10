use std::{fmt, fmt::Write, io};

use ntex_bytes::ByteString;

use crate::error::SpecViolation;

/// Check nonempty topic-filter syntax and wildcard placement without checking wire string limits.
#[allow(clippy::match_same_arms)]
pub(crate) fn is_valid(topic: &str) -> bool {
    if topic.is_empty() {
        false
    } else {
        /// Previous-byte category used to enforce wildcard level boundaries.
        enum PrevState {
            /// No byte has been examined yet.
            None,
            /// The previous byte was a level separator.
            LevelSep,
            /// A single-level wildcard must be followed by a separator or end of input.
            SingleWildcard,
            /// A multi-level wildcard must be the final byte.
            MultiWildcard,
            /// The previous byte belongs to a literal level.
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

/// One parsed topic-filter level, including wildcard and reserved-root distinctions.
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TopicFilterLevel {
    /// Nonempty literal level; a non-root level may start with `$`.
    Normal(ByteString),
    /// Reserved root level starting with `$`, also used after a shared-subscription prefix.
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

/// Match a sequence of subset levels against a covering filter, respecting reserved roots.
fn match_topic<T: MatchLevel, L: Iterator<Item = T>>(
    superset: &[TopicFilterLevel],
    subset: L,
) -> bool {
    let mut superset = superset.iter();

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

/// Returns the position of the filter after the `$share/{ShareName}/` prefix
/// of a Shared Subscription (MQTT 5.0, 4.8.2)
fn filter_start(levels: &[TopicFilterLevel]) -> usize {
    match levels {
        [TopicFilterLevel::System(s), _, ..] if s == "$share" => 2,
        _ => 0,
    }
}

/// Parsed MQTT topic filter with validated wildcard and shared-subscription syntax.
///
/// A Shared Subscription `$share/{ShareName}/{filter}` is parsed with the `$share`
/// and `ShareName` levels, the levels of the filter follow them (MQTT 5.0, 4.8.2)
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize)]
pub struct TopicFilter(Vec<TopicFilterLevel>);

impl<'de> serde::Deserialize<'de> for TopicFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // same format as the derived impl, the levels are validated
        /// Deserialize the serialized level sequence before validating it as a filter.
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

    /// Returns the `ShareName` of a Shared Subscription, `$share/{ShareName}/{filter}`
    pub fn share_name(&self) -> Option<&str> {
        match self.0.get(1) {
            Some(TopicFilterLevel::Normal(name)) if filter_start(&self.0) != 0 => Some(name),
            _ => None,
        }
    }

    /// Returns levels of the filter without the Shared Subscription prefix
    fn filter(&self) -> &[TopicFilterLevel] {
        &self.0[filter_start(&self.0)..]
    }

    /// Validate shared-subscription names and ensure levels form a nonempty filter.
    fn is_valid(&self) -> bool {
        let start = filter_start(&self.0);
        // [MQTT-4.8.2-1] the ShareName must be at least one character long
        // and must not contain "/", "+" or "#" (MQTT 5.0, 4.8.2)
        if start != 0
            && !matches!(&self.0[1], TopicFilterLevel::Normal(s) if !s.is_empty() && !s.contains(['/', '+', '#']))
        {
            return false;
        }

        // a topic filter is at least one character long, [MQTT-4.7.3-1] (MQTT 5.0, 4.7.3),
        // [MQTT-4.8.2-2] the ShareName is followed by a topic filter (MQTT 5.0, 4.8.2)
        let filter = &self.0[start..];
        !filter.is_empty()
            && filter != [TopicFilterLevel::Blank]
            && filter
                .iter()
                .enumerate()
                .all(|(pos, level)| level.is_valid(pos, filter.len()))
    }

    /// Check whether this filter covers the topics matched by another filter.
    ///
    /// Shared Subscriptions are matched by the filter after the `$share/{ShareName}/` prefix
    pub fn matches_filter(&self, topic: &TopicFilter) -> bool {
        match_topic(self.filter(), topic.filter().iter())
    }

    /// Check whether this filter matches the supplied topic name.
    ///
    /// Shared Subscriptions are matched by the filter after the `$share/{ShareName}/` prefix
    pub fn matches_topic<S: AsRef<str> + ?Sized>(&self, topic: &S) -> bool {
        match_topic(self.filter(), topic.as_ref().split('/'))
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

/// Compare parsed or string levels against a covering filter level.
trait MatchLevel {
    /// Check coverage at a zero-based filter position, excluding reserved roots from wildcards.
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

/// Compare parsed levels while honoring wildcard coverage and reserved-root exclusions.
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
        // the filter of a Shared Subscription starts after `$share/{ShareName}/`
        let start = if is_shared(&value) { 2 } else { 0 };

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
                    } else if (idx == 0 || idx == start) && is_system(level) {
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

/// Write topic-filter components using an ordinary byte writer.
#[allow(dead_code)]
pub(crate) trait WriteTopicExt: io::Write {
    /// Write one level and return the bytes accepted; an underlying write may be partial.
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

    /// Write filter levels and separators, returning the total bytes accepted.
    ///
    /// Each component uses `io::Write::write` and may be written only partially.
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

/// Check whether a level begins with the reserved-root marker `$`.
fn is_system<T: AsRef<str>>(s: T) -> bool {
    s.as_ref().starts_with('$')
}

/// Recover a zero-copy UTF-8 slice known to lie within the original byte string.
fn recover_bstr(superset: &ByteString, subset: &str) -> ByteString {
    unsafe { ByteString::from_bytes_unchecked(superset.as_bytes().slice_ref(subset.as_bytes())) }
}

/// Unit tests for topic-filter parsing, matching, serialization, and shared subscriptions.
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use test_case::test_case;

    /// STATEMENT: Topic-filter wildcards occupy complete levels and multi-level wildcards terminate the filter.
    ///
    /// HOW:
    /// - Pass literal, empty-level, and wildcard combinations to `is_valid`.
    /// - Compare the result with each case's expected syntax validity.
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

    /// Build a literal test level, rejecting wildcard characters.
    fn lvl_normal<T: AsRef<str>>(s: T) -> TopicFilterLevel {
        assert!(
            !s.as_ref().contains(['+', '#']),
            "invalid normal level `{}` contains +|#",
            s.as_ref()
        );
        TopicFilterLevel::Normal(s.as_ref().into())
    }

    /// Build a reserved-root test level, checking its prefix and absence of wildcards.
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

    /// Parse a static test filter, panicking when its syntax is invalid.
    fn topic(topic: &'static str) -> TopicFilter {
        TopicFilter::try_from(ByteString::from_static(topic)).unwrap()
    }

    /// STATEMENT: Parsing preserves level kinds and rejects malformed wildcard or shared-subscription syntax.
    ///
    /// HOW:
    /// - Parse ordinary, reserved-root, empty-level, wildcard, and shared-subscription filters.
    /// - Compare parsed level vectors or specific errors with each case's expectation.
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
    #[test_case("$share/g/a/+" => Ok(vec![lvl_sys("$share"), lvl_normal("g"), lvl_normal("a"), TopicFilterLevel::SingleWildcard]) ; "shared1")]
    #[test_case("$share/g/$SYS/#" => Ok(vec![lvl_sys("$share"), lvl_normal("g"), lvl_sys("$SYS"), TopicFilterLevel::MultiWildcard]) ; "shared2")]
    #[test_case("$share/$g/a/$b" => Ok(vec![lvl_sys("$share"), lvl_normal("$g"), lvl_normal("a"), lvl_normal("$b")]) ; "shared3")]
    #[test_case("$share/g//" => Ok(vec![lvl_sys("$share"), lvl_normal("g"), TopicFilterLevel::Blank, TopicFilterLevel::Blank]) ; "shared4")]
    #[test_case("$share" => Ok(vec![lvl_sys("$share")]) ; "shared5")]
    #[test_case("$sharex/g/$a" => Ok(vec![lvl_sys("$sharex"), lvl_normal("g"), lvl_normal("$a")]) ; "shared6")]
    #[test_case("$share/g" => Err(TopicFilterError::InvalidTopic) ; "shared_fail1")]
    #[test_case("$share/g/" => Err(TopicFilterError::InvalidTopic) ; "shared_fail2")]
    #[test_case("$share//a" => Err(TopicFilterError::InvalidTopic) ; "shared_fail3")]
    #[test_case("$share/+/a" => Err(TopicFilterError::InvalidTopic) ; "shared_fail4")]
    #[test_case("$share/#" => Err(TopicFilterError::InvalidTopic) ; "shared_fail5")]
    #[test_case("$share/" => Err(TopicFilterError::InvalidTopic) ; "shared_fail6")]
    #[test_case("$share/g/#/a" => Err(TopicFilterError::InvalidTopic) ; "shared_fail7")]
    fn parsing(input: &str) -> Result<Vec<TopicFilterLevel>, TopicFilterError> {
        TopicFilter::try_from(ByteString::from(input)).map(|t| t.levels().to_vec())
    }

    /// STATEMENT: Constructing a filter from levels enforces root, wildcard, and shared-subscription invariants.
    ///
    /// HOW:
    /// - Try level vectors including empty filters, misplaced reserved roots, malformed names, and shared prefixes.
    /// - Compare construction success with each case's expected validity.
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
    #[test_case(vec![TopicFilterLevel::Blank] => false; "17")]
    #[test_case(vec![TopicFilterLevel::Blank, TopicFilterLevel::Blank] => true; "18")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g"), lvl_sys("$SYS")] => true; "shared1")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g"), lvl_normal("$SYS")] => false; "shared2")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g"), lvl_normal("a"), lvl_sys("$b")] => false; "shared3")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g"), TopicFilterLevel::Blank] => false; "shared4")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g")] => false; "shared5")]
    #[test_case(vec![lvl_sys("$share"), TopicFilterLevel::Blank, lvl_normal("a")] => false; "shared6")]
    #[test_case(vec![lvl_sys("$share"), TopicFilterLevel::SingleWildcard, lvl_normal("a")] => false; "shared7")]
    #[test_case(vec![lvl_sys("$share"), TopicFilterLevel::Normal("a+".into()), lvl_normal("a")] => false; "shared8")]
    #[test_case(vec![lvl_sys("$share"), TopicFilterLevel::MultiWildcard] => false; "shared9")]
    #[test_case(vec![lvl_sys("$share"), lvl_normal("g"), TopicFilterLevel::MultiWildcard] => true; "shared10")]
    fn topic_is_valid(levels: Vec<TopicFilterLevel>) -> bool {
        TopicFilter::try_from(levels).is_ok()
    }

    /// STATEMENT: Shared-subscription parsing exposes its group and agrees with incoming-filter syntax checks.
    ///
    /// HOW:
    /// - Check group extraction, nonshared lookalikes, and shared-filter display output.
    /// - Compare parsing success against combined ordinary and shared syntax checks for valid and invalid filters.
    #[test]
    fn test_shared_subscription() {
        assert_eq!(topic("$share/g/a").share_name(), Some("g"));
        assert_eq!(topic("$share/$g/a").share_name(), Some("$g"));
        assert_eq!(topic("$share").share_name(), None);
        assert_eq!(topic("$sharex/g/a").share_name(), None);
        assert_eq!(topic("a/$share/g").share_name(), None);
        assert_eq!(topic("$share/g/a/+").to_string(), "$share/g/a/+");

        // parsing agrees with the checks of the incoming filters
        for filter in [
            "$share",
            "$share/",
            "$share/g",
            "$share/g/",
            "$share//a",
            "$share/g/a",
            "$share/g//",
            "$share/g/#",
            "$share/+/a",
            "$share/#",
            "$share/g+/a",
            "$share/g/a#",
            "$share/g/$SYS/+",
            "$sharex/g",
            "/",
            "a/$share/g",
        ] {
            assert_eq!(
                TopicFilter::from_str(filter).is_ok(),
                is_valid(filter) && is_valid_shared(filter),
                "{filter}"
            );
        }
    }

    /// STATEMENT: An empty level sequence cannot form a topic filter.
    ///
    /// HOW:
    /// - Construct filters from an empty vector and an empty slice.
    /// - Assert that both return `TopicFilterError::InvalidTopic`.
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

    /// STATEMENT: Topic-filter serialization round-trips valid levels and rejects invalid deserialized sequences.
    ///
    /// HOW:
    /// - Round-trip reserved-root and shared filters through JSON and compare values and display output.
    /// - Attempt JSON sequences with empty, malformed, or misplaced levels and assert rejection.
    #[test]
    fn test_serde() {
        for filter in ["$SYS/+/a//#", "$share/g/$SYS/+"] {
            let tf = topic(filter);
            let json = serde_json::to_string(&tf).unwrap();
            let de: TopicFilter = serde_json::from_str(&json).unwrap();
            assert_eq!(de, tf);
            assert_eq!(de.to_string(), filter);
        }

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

    /// STATEMENT: A multi-level wildcard covers itself and a parent topic without additional levels.
    ///
    /// HOW:
    /// - Compare `sport/tennis/#` with its equivalent parsed filter and the parent topic.
    /// - Assert that the root `#` filter also covers itself.
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

    /// STATEMENT: Equivalent parsed single-wildcard filters match each other.
    ///
    /// HOW:
    /// - Construct matching filters with root and interior single-level wildcards, including a trailing `#`.
    /// - Assert each equivalent filter is covered.
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

    /// STATEMENT: Writing and formatting a topic filter preserve wildcard text and level separators.
    ///
    /// HOW:
    /// - Build `+/tennis/#` from levels and write it into a byte vector.
    /// - Check the returned byte count, written bytes, display formatting, and string conversion.
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

    /// STATEMENT: Topic matching respects wildcard depth, reserved roots, and shared-subscription prefixes.
    ///
    /// HOW:
    /// - Match each parsed filter against its supplied topic name.
    /// - Compare the result with expectations for literal, wildcard, empty-level, reserved-root, and shared cases.
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
    #[test_case("$share/g/a/+", "a/b" => true; "shared1")]
    #[test_case("$share/g/#", "a/b" => true; "shared2")]
    #[test_case("$share/g/#", "$SYS/a" => false; "shared3")]
    #[test_case("$share/g/+/a", "$SYS/a" => false; "shared4")]
    #[test_case("$share/g/$SYS/#", "$SYS/a" => true; "shared5")]
    #[test_case("$share/g/a", "$share/g/a" => false; "shared6")]
    #[test_case("$share", "$share" => true; "shared7")]
    fn matches_topic(filter: &'static str, topic_str: &'static str) -> bool {
        topic(filter).matches_topic(topic_str)
    }

    /// STATEMENT: Filter coverage respects wildcard breadth and ignores shared group names without ignoring reserved roots.
    ///
    /// HOW:
    /// - Parse each superset/subset pair and call `TopicFilter::matches_filter`.
    /// - Compare coverage with expectations for literal levels, wildcard combinations, reserved roots, and shared prefixes.
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
    #[test_case("$share/g/#", "a/b" => true; "shared1")]
    #[test_case("#", "$share/g/a" => true; "shared2")]
    #[test_case("a/+", "$share/g/a/b" => true; "shared3")]
    #[test_case("$share/g/+", "$share/h/a" => true; "shared4")]
    #[test_case("#", "$share/g/$SYS/a" => false; "shared5")]
    #[test_case("$SYS/+", "$share/g/$SYS/a" => true; "shared6")]
    #[test_case("$share/g/a", "$share/g/b" => false; "shared7")]
    #[test_case("$share/g/+", "#" => false; "shared8")]
    fn matches_filter(superset_filter: &'static str, subset_filter: &'static str) -> bool {
        topic(superset_filter).matches_filter(&topic(subset_filter))
    }

    /// STATEMENT: Shared syntax requires a nonempty wildcard-free group and a nonempty trailing filter.
    ///
    /// HOW:
    /// - Check ordinary filters, valid shared prefixes, missing components, and wildcard-containing group names.
    /// - Compare `is_valid_shared` with each case's expected validity.
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

    /// STATEMENT: Owned filter levels retain empty-level and reserved-root subset semantics.
    ///
    /// HOW:
    /// - Convert parsed filters into owned levels and match them against covering filters.
    /// - Compare with borrowed matching for blanks, wildcards, and reserved roots, including mismatches.
    #[test]
    fn test_owned_levels_preserve_subset_matching() {
        for (cover, subset, expected) in [
            ("a//+", "a//b", true),
            ("a//+", "a/b/c", false),
            ("a//+", "a/+/c", false),
            ("a/+/+", "a//b", true),
            ("#", "$SYS/a", false),
            ("+/a", "$SYS/a", false),
            ("$SYS/#", "$SYS/a", true),
            ("a/+", "a/#", false),
            ("a/#", "a", true),
        ] {
            let covering = topic(cover);
            let parsed = topic(subset);
            let levels: Vec<TopicFilterLevel> = parsed.clone().into();
            assert_eq!(
                match_topic(covering.filter(), levels.into_iter()),
                expected,
                "{cover} covers {subset}"
            );
            assert_eq!(covering.matches_filter(&parsed), expected);
        }
    }

    /// STATEMENT: Writing blank topic levels preserves leading, interior, and trailing separators.
    ///
    /// HOW:
    /// - Write parsed filters containing blank levels to a byte vector.
    /// - Check the returned byte count and bytes against the original filter text.
    #[test]
    fn test_write_blank_topic_levels() {
        for filter in ["/", "//", "/a//+/#", "$SYS//", "a/"] {
            let mut bytes = Vec::new();
            assert_eq!(bytes.write_topic(&topic(filter)).unwrap(), filter.len());
            assert_eq!(bytes, filter.as_bytes());
        }
    }
}
