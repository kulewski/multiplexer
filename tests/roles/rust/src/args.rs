//! The roles' command lines, as tests/README.md's role contract has the
//! harness pass them: `--option value` or `--option=value`, an option given
//! again adding a value, and bare switches. An option the role does not
//! take is a usage error, which ends the role with exit code 2 as the
//! other roles' parsers end theirs.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

/// The options every role takes (tests/README.md).
pub const COMMON: [&str; 3] = ["mx", "type", "name"];

/// A parsed command line.
#[derive(Debug, Default)]
pub struct Args {
    values: BTreeMap<String, Vec<String>>,
    switches: BTreeSet<String>,
}

impl Args {
    /// Parses `argv`, the arguments after the role's name, for a role that
    /// takes the options `valued`, besides the common ones, and the
    /// switches `switches`.
    pub fn parse(argv: &[String], valued: &[&str], switches: &[&str]) -> Result<Args, String> {
        let mut args = Args::default();
        let mut arguments = argv.iter();
        while let Some(argument) = arguments.next() {
            let Some(option) = argument.strip_prefix("--") else {
                return Err(format!("unexpected argument {argument:?}"));
            };
            let (name, inline) = match option.split_once('=') {
                Some((name, value)) => (name, Some(value.to_string())),
                None => (option, None),
            };
            if switches.contains(&name) {
                if inline.is_some() {
                    return Err(format!("--{name} takes no value"));
                }
                args.switches.insert(name.to_string());
                continue;
            }
            if !valued.contains(&name) && !COMMON.contains(&name) {
                return Err(format!("unknown option --{name}"));
            }
            let value = match inline {
                Some(value) => value,
                None => arguments.next().ok_or_else(|| format!("--{name} needs a value"))?.clone(),
            };
            args.values.entry(name.to_string()).or_default().push(value);
        }
        Ok(args)
    }

    /// Every value given to `--name`, in order.
    pub fn all(&self, name: &str) -> &[String] {
        self.values.get(name).map_or(&[], Vec::as_slice)
    }

    /// The last value given to `--name`, read as a `T`; `default` when the
    /// option was not given.
    pub fn get<T: FromStr>(&self, name: &str, default: T) -> Result<T, String> {
        match self.all(name).last() {
            None => Ok(default),
            Some(value) => value.parse().map_err(|_| format!("--{name}: cannot read {value:?}")),
        }
    }

    /// The last value given to `--name`, empty when it was not given.
    pub fn text(&self, name: &str) -> String {
        self.all(name).last().cloned().unwrap_or_default()
    }

    /// Whether the switch `--name` was given.
    pub fn switch(&self, name: &str) -> bool {
        self.switches.contains(name)
    }
}

#[cfg(test)]
mod tests {
    //! The forms the harness passes, and the mistakes refused.

    use super::Args;

    /// `text` split at spaces, as a command line.
    fn argv(text: &str) -> Vec<String> {
        text.split(' ').map(str::to_string).collect()
    }

    #[test]
    fn values_repeat_switches_stand_alone_and_both_forms_read() {
        let args = Args::parse(
            &argv("--mx 127.0.0.1:1 --mx=[::1]:2 --type 203 --threaded --count 3"),
            &["count"],
            &["threaded"],
        )
        .unwrap();
        assert_eq!(["127.0.0.1:1", "[::1]:2"], args.all("mx"));
        assert_eq!(Ok(203u32), args.get("type", 0));
        assert_eq!(Ok(3i64), args.get("count", 1));
        assert_eq!(Ok(10.0), args.get("timeout", 10.0));
        assert!(args.switch("threaded"));
        assert_eq!("", args.text("name"));
    }

    #[test]
    fn an_unknown_option_a_missing_value_and_a_bad_number_are_refused() {
        assert!(Args::parse(&argv("--type 1 --nope 2"), &[], &[]).is_err());
        assert!(Args::parse(&argv("--type"), &[], &[]).is_err());
        assert!(Args::parse(&argv("--threaded=1"), &[], &["threaded"]).is_err());
        let args = Args::parse(&argv("--type x"), &[], &[]).unwrap();
        assert!(args.get("type", 0u32).is_err());
    }
}
