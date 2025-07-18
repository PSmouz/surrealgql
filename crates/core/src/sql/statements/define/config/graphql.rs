use std::fmt::{self, Display, Write};

use crate::sql::fmt::{pretty_indent, Fmt, Pretty};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Ident, Part, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct GraphQLConfig {
    pub tables: TablesConfig,
    pub functions: FunctionsConfig,
    pub cursor: CursorConfig,
    pub introspection: IntrospectionConfig,
    pub limits: LimitsConfig,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum TablesConfig {
    #[default]
    None,
    Auto,
    Include(Vec<TableConfig>),
    Exclude(Vec<TableConfig>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct TableConfig {
    pub name: String,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum FunctionsConfig {
    #[default]
    None,
    Auto,
    Include(Vec<Ident>),
    Exclude(Vec<Ident>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum CursorConfig {
    None,
    #[default]
    Auto,
    Relation,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum IntrospectionConfig {
    #[default]
    None,
    Auto,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum LimitsConfig {
    None,
    #[default]
    Auto,
    To(Vec<LimitConfig>),
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum LimitConfig {
    Complexity(usize),
    Depth(usize),
    RecursiveDepth(usize),
    Directives(usize),
}

impl Display for GraphQLConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " GRAPHQL")?;

        write!(f, " TABLES {}", self.tables)?;
        write!(f, " FUNCTIONS {}", self.functions)?;
        write!(f, " CURSOR {}", self.cursor)?;
        write!(f, " INTROSPECTION {}", self.introspection)?;
        write!(f, " LIMIT {}", self.limits)?;
        Ok(())
    }
}

impl Display for TablesConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TablesConfig::Auto => write!(f, "AUTO")?,
            TablesConfig::None => write!(f, "NONE")?,
            TablesConfig::Include(cs) => {
                let mut f = Pretty::from(f);
                write!(f, "INCLUDE [")?;
                if !cs.is_empty() {
                    let indent = pretty_indent();
                    write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
                    drop(indent);
                }
                f.write_char(']')?;
            }
            TablesConfig::Exclude(cs) => {
                let mut f = Pretty::from(f);
                write!(f, "EXCLUDE [")?;
                if !cs.is_empty() {
                    let indent = pretty_indent();
                    write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
                    drop(indent);
                }
                f.write_char(']')?;
            }
        }

        Ok(())
    }
}

impl From<String> for TableConfig {
    fn from(value: String) -> Self {
        Self {
            name: value,
        }
    }
}

pub fn val_to_ident(val: Value) -> Result<Ident, Value> {
    match val {
        Value::Strand(s) => Ok(s.0.into()),
        Value::Table(n) => Ok(n.0.into()),
        Value::Idiom(ref i) => match &i[..] {
            [Part::Field(n)] => Ok(n.to_raw().into()),
            _ => Err(val),
        },
        _ => Err(val),
    }
}

impl TryFrom<Value> for TableConfig {
    type Error = Value;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            v @ Value::Strand(_) | v @ Value::Table(_) | v @ Value::Idiom(_) => {
                val_to_ident(v).map(|i| i.0.into())
            }
            _ => Err(value),
        }
    }
}

impl Display for TableConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)?;
        Ok(())
    }
}

impl Display for FunctionsConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FunctionsConfig::Auto => write!(f, "AUTO")?,
            FunctionsConfig::None => write!(f, "NONE")?,
            FunctionsConfig::Include(cs) => {
                let mut f = Pretty::from(f);
                write!(f, "INCLUDE [")?;
                if !cs.is_empty() {
                    let indent = pretty_indent();
                    write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
                    drop(indent);
                }
                f.write_char(']')?;
            }
            FunctionsConfig::Exclude(cs) => {
                let mut f = Pretty::from(f);
                write!(f, "EXCLUDE [")?;
                if !cs.is_empty() {
                    let indent = pretty_indent();
                    write!(f, "{}", Fmt::pretty_comma_separated(cs.as_slice()))?;
                    drop(indent);
                }
                f.write_char(']')?;
            }
        }

        Ok(())
    }
}

impl Display for CursorConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CursorConfig::Auto => write!(f, "AUTO")?,
            CursorConfig::None => write!(f, "NONE")?,
            CursorConfig::Relation => write!(f, "RELATION")?,
        }

        Ok(())
    }
}

impl Display for IntrospectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IntrospectionConfig::Auto => write!(f, "AUTO")?,
            IntrospectionConfig::None => write!(f, "NONE")?,
        }

        Ok(())
    }
}

impl Display for LimitsConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LimitsConfig::None => write!(f, "NONE")?,
            LimitsConfig::Auto => write!(f, "AUTO")?,
            LimitsConfig::To(limits) => {
                let mut f = Pretty::from(f);
                write!(f, "TO ")?;
                if !limits.is_empty() {
                    let indent = pretty_indent();
                    write!(f, "{}", Fmt::pretty_comma_separated(limits.as_slice()))?;
                    drop(indent);
                }
            }
        }

        Ok(())
    }
}

impl Display for LimitConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LimitConfig::Complexity(c) => write!(f, "COMPLEXITY {}", c)?,
            LimitConfig::Depth(d) => write!(f, "DEPTH {}", d)?,
            LimitConfig::RecursiveDepth(rd) => write!(f, "RECURSIVE_DEPTH {}", rd)?,
            LimitConfig::Directives(d) => write!(f, "DIRECTIVES {}", d)?,
        }

        Ok(())
    }
}

impl InfoStructure for GraphQLConfig {
    fn structure(self) -> Value {
        Value::from(map!(
            "tables" => self.tables.structure(),
            "functions" => self.functions.structure(),
            "cursor" => self.cursor.structure(),
            "introspection" => self.introspection.structure(),
            "limits" => self.limits.structure(),
        ))
    }
}

impl InfoStructure for TablesConfig {
    fn structure(self) -> Value {
        match self {
            TablesConfig::None => Value::None,
            TablesConfig::Auto => Value::Strand("AUTO".into()),
            TablesConfig::Include(ts) => Value::from(map!(
				"include" => Value::Array(ts.into_iter().map(InfoStructure::structure).collect()),
			)),
            TablesConfig::Exclude(ts) => Value::from(map!(
				"exclude" => Value::Array(ts.into_iter().map(InfoStructure::structure).collect()),
			)),
        }
    }
}

impl InfoStructure for TableConfig {
    fn structure(self) -> Value {
        Value::from(map!(
			"name" => Value::from(self.name),
		))
    }
}

impl InfoStructure for FunctionsConfig {
    fn structure(self) -> Value {
        match self {
            FunctionsConfig::None => Value::None,
            FunctionsConfig::Auto => Value::Strand("AUTO".into()),
            FunctionsConfig::Include(fs) => Value::from(map!(
				"include" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
            FunctionsConfig::Exclude(fs) => Value::from(map!(
				"exclude" => Value::Array(fs.into_iter().map(|i| Value::from(i.to_raw())).collect()),
			)),
        }
    }
}

impl InfoStructure for CursorConfig {
    fn structure(self) -> Value {
        match self {
            CursorConfig::None => Value::None,
            CursorConfig::Auto => Value::Strand("AUTO".into()),
            CursorConfig::Relation => Value::Strand("RELATION".into()),
        }
    }
}

impl InfoStructure for IntrospectionConfig {
    fn structure(self) -> Value {
        match self {
            IntrospectionConfig::None => Value::None,
            IntrospectionConfig::Auto => Value::Strand("AUTO".into()),
        }
    }
}

impl InfoStructure for LimitsConfig {
    fn structure(self) -> Value {
        match self {
            LimitsConfig::None => Value::None,
            LimitsConfig::Auto => Value::Strand("AUTO".into()),
            LimitsConfig::To(limits) => Value::from(map!(
                "to" => Value::Array(limits.into_iter().map(InfoStructure::structure).collect()),
            )),
        }
    }
}

impl InfoStructure for LimitConfig {
    fn structure(self) -> Value {
        match self {
            LimitConfig::Complexity(c) => Value::from(map!(
                "complexity" => Value::from(c),
            )),
            LimitConfig::Depth(d) => Value::from(map!(
                "depth" => Value::from(d),
            )),
            LimitConfig::RecursiveDepth(rd) => Value::from(map!(
                "recursive_depth" => Value::from(rd),
            )),
            LimitConfig::Directives(d) => Value::from(map!(
                "directives" => Value::from(d),
            )),
        }
    }
}

// DEFINE CONFIG GRAPHQL IF NOT EXISTS AUTO CURSOR;
// DEFINE CONFIG [ OVERWRITE | IF NOT EXISTS ]
//
// [ API [ MIDDLEWARE @expression, .. ] [ PERMISSIONS [ NONE | FULL | @expression ] ]
//
// [ GRAPHQL
// [ AUTO | NONE ]
// [ TABLES (AUTO | NONE | INCLUDE table1, table2, ...) ]
// [ FUNCTIONS (AUTO | NONE | INCLUDE [function1, function2, ...] | EXCLUDE [function1, function2, ...]) ] ]
// [ CURSOR ( AUTO | NONE | RELATIONS )] ]
// auto = all, none = no, relations = only relation tables are added as cursor conn to its in table
// INTROSPECTION bool? oder AUTO | NONE
// LIMIT COMPLEXITY int>0
// LIMIT DEPTH int>0
// LIMIT RECURSIVE DEPTH int > 0 auto set to < 32
// LIMIT DIRECTIVES int>0 default no