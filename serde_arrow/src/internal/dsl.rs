//! A minimal config language to describe Strategies and data types
use std::str::FromStr;

use crate::internal::error::{fail, Error, Result};

#[derive(Debug, Default, PartialEq, Clone, Eq, PartialOrd, Ord)]
pub struct Term {
    pub name: String,
    pub quoted: bool,
    pub arguments: Vec<Term>,
}

impl Term {
    pub fn as_ident(&self) -> Result<&str> {
        match (self.name.as_str(), self.quoted, self.arguments.as_slice()) {
            (name, false, []) => Ok(name),
            (_, true, _) => fail!("expected identifier, found quoted string"),
            (_, _, [_, ..]) => fail!("expected identifier, found call"),
        }
    }

    pub fn as_string(&self) -> Result<&str> {
        match (self.name.as_str(), self.quoted, self.arguments.as_slice()) {
            (name, true, []) => Ok(name),
            (_, false, _) => fail!("expected string, found identifier"),
            (_, _, [_, ..]) => fail!("expected identifier, found call"),
        }
    }

    pub fn as_option(&self) -> Result<Option<&Term>> {
        match (self.name.as_str(), self.quoted, self.arguments.as_slice()) {
            ("None", false, []) => Ok(None),
            ("Some", false, [arg]) => Ok(Some(arg)),
            _ => fail!("expected Some(arg) or None found quoted string"),
        }
    }

    pub fn as_call(&self) -> Result<(&str, &[Term])> {
        match (self.name.as_str(), self.quoted, self.arguments.as_slice()) {
            (name, false, args) => Ok((name, args)),
            (_, true, _) => fail!("expected call, found quoted string"),
        }
    }
}

#[cfg(test)]
impl Term {
    pub fn ident<S: Into<String>>(s: S) -> Self {
        Self {
            name: s.into(),
            quoted: false,
            arguments: Vec::new(),
        }
    }

    pub fn quoted<S: Into<String>>(s: S) -> Self {
        Self {
            name: s.into(),
            quoted: true,
            arguments: Vec::new(),
        }
    }

    pub fn args<A: Into<Vec<Term>>>(mut self, args: A) -> Self {
        self.arguments = args.into();
        self
    }
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.quoted {
            write!(f, "{name:?}", name = self.name)?;
        } else {
            write!(f, "{name}", name = self.name)?;
        }

        if !self.arguments.is_empty() {
            write!(f, "(")?;
            for (idx, arg) in self.arguments.iter().enumerate() {
                if idx != 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{arg}")?;
            }
            write!(f, ")")?;
        }

        Ok(())
    }
}

impl FromStr for Term {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (res, rest) = parse_term(s)?;
        if !rest.trim().is_empty() {
            fail!("Trailing content in term: {rest:?}");
        }
        Ok(res)
    }
}

fn parse_term(s: &str) -> Result<(Term, &str)> {
    let s = s.trim_start();
    let (name, quoted, s) = parse_term_name(s)?;
    let s = s.trim_start();
    let (arguments, s) = parse_arguments(s)?;

    Ok((
        Term {
            name,
            quoted,
            arguments,
        },
        s,
    ))
}

fn parse_term_name(s: &str) -> Result<(String, bool, &str)> {
    if s.starts_with('"') {
        let (name, s) = parse_quoted_term_name(s)?;
        Ok((name, true, s))
    } else {
        let (name, s) = parse_ident_term_name(s)?;
        Ok((name, false, s))
    }
}

fn parse_quoted_term_name(s: &str) -> Result<(String, &str)> {
    let Some(s) = s.strip_prefix('"') else {
        fail!("Missing start quote");
    };

    let mut quoted = false;
    let Some((end, quote)) = s.char_indices().find(|(_, c)| {
        if quoted {
            quoted = false;
            true
        } else if *c == '\\' {
            quoted = true;
            true
        } else {
            *c == '"'
        }
    }) else {
        fail!("Missing end quote");
    };

    let ident = s[..end].to_owned();
    let s = &s[end + quote.len_utf8()..];

    Ok((ident, s))
}

fn parse_ident_term_name(s: &str) -> Result<(String, &str)> {
    let pos = s
        .find(|c: char| !c.is_alphanumeric() && !matches!(c, '-' | '+'))
        .unwrap_or(s.len());
    let ident = s[..pos].to_string();
    let rest = &s[pos..];

    if ident.is_empty() {
        fail!("no identifier found");
    }

    Ok((ident, rest))
}

fn parse_arguments(s: &str) -> Result<(Vec<Term>, &str)> {
    let Some(s) = s.strip_prefix('(') else {
        return Ok((vec![], s));
    };

    let mut s = s;
    let mut arguments = Vec::new();

    loop {
        s = s.trim_start();

        let term;
        (term, s) = parse_term(s)?;
        arguments.push(term);

        s = s.trim_start();
        s = match s.strip_prefix(',') {
            Some(s) => s,
            None => break,
        };
    }

    let s = s.trim_start();
    let Some(s) = s.strip_prefix(')') else {
        fail!("mising ')'");
    };

    Ok((arguments, s))
}

#[cfg(test)]
mod test {
    use super::Term;
    use std::str::FromStr;

    fn assert_term_eq(str: &str, term: Term) {
        assert_eq!(Term::from_str(str).unwrap(), term);
        assert_eq!(term.to_string(), str);
    }

    fn parse(s: &str) -> Term {
        Term::from_str(s).unwrap()
    }

    #[test]
    fn example() {
        assert_term_eq("InconsistentTypes", Term::ident("InconsistentTypes"));
    }

    #[test]
    fn example_whitespace() {
        let term = Term::ident("InconsistentTypes");
        assert_eq!(parse("InconsistentTypes"), term);
        assert_eq!(parse("  InconsistentTypes"), term);
        assert_eq!(parse("InconsistentTypes  "), term);
        assert_eq!(parse(" InconsistentTypes "), term)
    }

    #[test]
    fn complex() {
        assert_term_eq(
            "Timestamp(Seconds, Some(\"Utc\"))",
            Term::ident("Timestamp").args([
                Term::ident("Seconds"),
                Term::ident("Some").args([Term::quoted("Utc")]),
            ]),
        );
    }

    #[test]
    fn complex_whitespace() {
        let term = Term::ident("Timestamp").args([
            Term::ident("Seconds"),
            Term::ident("Some").args([Term::quoted("Utc")]),
        ]);
        assert_eq!(parse("Timestamp(Seconds, Some(\"Utc\"))"), term);
        assert_eq!(parse("Timestamp(Seconds,Some(\"Utc\"))"), term);
        assert_eq!(parse(" Timestamp(Seconds, Some(\"Utc\"))"), term);
        assert_eq!(parse("Timestamp (Seconds, Some(\"Utc\"))"), term);
        assert_eq!(parse("Timestamp( Seconds, Some(\"Utc\"))"), term);
        assert_eq!(parse("Timestamp(Seconds , Some(\"Utc\"))"), term);
        assert_eq!(parse("Timestamp(Seconds, Some (\"Utc\"))"), term);
        assert_eq!(parse("Timestamp(Seconds, Some( \"Utc\"))"), term);
        assert_eq!(parse("Timestamp(Seconds, Some(\"Utc\" ))"), term);
        assert_eq!(parse("Timestamp(Seconds, Some(\"Utc\") )"), term);
        assert_eq!(parse("Timestamp(Seconds, Some(\"Utc\")) "), term);
    }
}
